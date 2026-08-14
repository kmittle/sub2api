package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

const (
	QuotaRecoverySlotSettingKey                   = "quota_recovery_last_utc_slot"
	QuotaRecoveryAntigravityUsageSnapshotExtraKey = "antigravity_usage_snapshot"

	quotaRecoveryInterval             = 4 * time.Hour
	quotaRecoveryFailureRetryInterval = 5 * time.Minute
	quotaRecoveryCycleTimeout         = 45 * time.Minute
	quotaRecoveryLeaderLockTTL        = 50 * time.Minute
	quotaRecoveryLeaderLockKey        = "quota:recovery:leader"
	quotaRecoveryPageSize             = 100
	quotaRecoveryConcurrency          = 2
	quotaRecoverySlotReleaseTimeout   = 5 * time.Second
)

// QuotaRecoveryAccountPageOptions describes one bounded, cursor-stable scan.
type QuotaRecoveryAccountPageOptions struct {
	AfterID int64
	Limit   int
}

type QuotaRecoveryAccountPage struct {
	Accounts    []Account
	NextAfterID int64
	HasMore     bool
}

// QuotaRecoveryMutation is an optimistic update based on the exact network
// identity and blocking generation observed before an upstream quota probe.
// Repository implementations must apply it atomically and return false on a
// comparison miss.
type QuotaRecoveryMutation struct {
	Target   *Account
	Identity *Account

	ExtraUpdates     map[string]any
	SessionWindowEnd *time.Time

	ClearGlobalRateLimit     bool
	ExpectedRateLimitedAt    *time.Time
	ExpectedRateLimitResetAt *time.Time

	ClearThresholdBlock bool
	ExpectedTempUntil   *time.Time
	ExpectedTempReason  string

	ClearModelRateLimitKeys []string
	ExpectedModelRateLimits map[string]any

	// ClearQuotaError recovers only the fixed balance-exhaustion errors written
	// by RateLimitService. Those errors use SetError, which changes both status
	// and the manual schedulable column, so clearing ordinary rate-limit fields
	// alone cannot make the account usable again.
	ClearQuotaError bool
}

func (m QuotaRecoveryMutation) clearsSchedulingBlock() bool {
	return m.ClearGlobalRateLimit || m.ClearThresholdBlock || len(m.ClearModelRateLimitKeys) > 0 || m.ClearQuotaError
}

// QuotaRecoveryRepository is deliberately narrower than AccountRepository so
// the periodic runner cannot accidentally call broad recovery helpers.
type QuotaRecoveryRepository interface {
	GetByID(context.Context, int64) (*Account, error)
	ClaimQuotaRecoverySlot(context.Context, time.Time) (bool, error)
	ReleaseQuotaRecoverySlot(context.Context, time.Time) (bool, error)
	ListQuotaRecoveryAccountPage(context.Context, QuotaRecoveryAccountPageOptions) (*QuotaRecoveryAccountPage, error)
	ApplyQuotaRecoveryMutation(context.Context, QuotaRecoveryMutation) (bool, error)
}

// QuotaRecoveryAccountTester is the read-only connectivity probe used for
// blocked accounts whose credential type does not expose an authoritative
// quota endpoint.
type QuotaRecoveryAccountTester interface {
	RunTestBackground(context.Context, int64, string) (*ScheduledTestResult, error)
}

// QuotaRecoveryRuntimeBlockSnapshot identifies one exact in-memory scheduling
// block. It is intentionally opaque to the periodic runner except for passing
// it back to the conditional clear operation.
type QuotaRecoveryRuntimeBlockSnapshot struct {
	Generation uint64
	Until      time.Time
	Reason     string
}

// QuotaRecoveryRuntimeBlocker is an optional, fail-closed extension used by
// the periodic runner. Implementations must clear only when the complete
// snapshot is still current and its reason matches the requested quota class.
type QuotaRecoveryRuntimeBlocker interface {
	SnapshotQuotaRecoveryRuntimeBlock(accountID int64) (QuotaRecoveryRuntimeBlockSnapshot, bool)
	ClearQuotaRecoveryRuntimeBlock(
		accountID int64,
		expected QuotaRecoveryRuntimeBlockSnapshot,
		clearGlobalRateLimit bool,
		clearThresholdBlock bool,
		clearQuotaError bool,
	) bool
}

// QuotaRecoveryTempUnschedCache is an optional, fail-closed extension. The
// normal TempUnschedCache delete is deliberately not used by quota recovery,
// because a new temporary block may be written after the database CAS.
type QuotaRecoveryTempUnschedCache interface {
	DeleteTempUnschedIfMatch(context.Context, int64, *TempUnschedState) (bool, error)
}

type quotaRecoveryRuntimeBlockContextKey struct{}

type quotaRecoveryRuntimeBlockObservation struct {
	accountID int64
	snapshot  QuotaRecoveryRuntimeBlockSnapshot
	present   bool
}

type quotaRecoveryCycleReport struct {
	AccountsScanned int
	ProbesAttempted int
	SnapshotsSaved  int
	BlocksCleared   int
	Errors          int
}

type quotaRecoveryAccountResult struct {
	probes    int
	snapshots int
	cleared   int
	err       error
}

// QuotaRecoveryService refreshes authoritative provider quota snapshots every
// four hours and clears only quota-derived runtime blocks whose observed
// generation is still current.
type QuotaRecoveryService struct {
	repo           QuotaRecoveryRepository
	usageService   *AccountUsageService
	accountTest    QuotaRecoveryAccountTester
	settings       *SettingService
	tempCache      TempUnschedCache
	runtimeBlocker AccountRuntimeBlocker
	lockCache      LeaderLockCache
	db             *sql.DB
	instanceID     string

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.Mutex
	cycle   sync.Mutex
	started bool
	stopped bool
}

func NewQuotaRecoveryService(
	repo QuotaRecoveryRepository,
	usageService *AccountUsageService,
	accountTest QuotaRecoveryAccountTester,
	settings *SettingService,
	tempCache TempUnschedCache,
	runtimeBlocker AccountRuntimeBlocker,
	lockCache LeaderLockCache,
	db *sql.DB,
) *QuotaRecoveryService {
	ctx, cancel := context.WithCancel(context.Background())
	return &QuotaRecoveryService{
		repo:           repo,
		usageService:   usageService,
		accountTest:    accountTest,
		settings:       settings,
		tempCache:      tempCache,
		runtimeBlocker: runtimeBlocker,
		lockCache:      lockCache,
		db:             db,
		instanceID:     uuid.NewString(),
		ctx:            ctx,
		cancel:         cancel,
	}
}

// ProvideQuotaRecoveryService starts the singleton periodic runner. Production
// fails closed if its AccountRepository lacks the narrow CAS contract.
func ProvideQuotaRecoveryService(
	accountRepo AccountRepository,
	usageService *AccountUsageService,
	accountTest *AccountTestService,
	settings *SettingService,
	tempCache TempUnschedCache,
	runtimeBlocker AccountRuntimeBlocker,
	lockCache LeaderLockCache,
	db *sql.DB,
) *QuotaRecoveryService {
	repo, ok := accountRepo.(QuotaRecoveryRepository)
	if !ok {
		slog.Error("quota_recovery_repository_unavailable")
		return NewQuotaRecoveryService(nil, usageService, accountTest, settings, tempCache, runtimeBlocker, lockCache, db)
	}
	svc := NewQuotaRecoveryService(repo, usageService, accountTest, settings, tempCache, runtimeBlocker, lockCache, db)
	svc.Start()
	return svc
}

func (s *QuotaRecoveryService) Start() {
	if s == nil || s.repo == nil {
		return
	}
	s.mu.Lock()
	if s.started || s.stopped {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.wg.Add(1)
	s.mu.Unlock()
	go s.runLoop()
}

func (s *QuotaRecoveryService) Stop() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.cancel()
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *QuotaRecoveryService) runLoop() {
	defer s.wg.Done()
	for {
		err := s.RunOnce(s.ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("quota_recovery_cycle_failed", "error", err)
		}
		timer := time.NewTimer(quotaRecoveryRunLoopWait(time.Now(), err))
		select {
		case <-s.ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func quotaRecoveryRunLoopWait(now time.Time, cycleErr error) time.Duration {
	now = now.UTC()
	next := now.Truncate(quotaRecoveryInterval).Add(quotaRecoveryInterval)
	wait := next.Sub(now)
	if wait <= 0 {
		wait = quotaRecoveryInterval
	}
	if cycleErr != nil && wait > quotaRecoveryFailureRetryInterval {
		return quotaRecoveryFailureRetryInterval
	}
	return wait
}

// RunOnce attempts the current UTC four-hour slot. Individual account failures
// are best effort; scan-level failures release the exact slot for a short retry.
func (s *QuotaRecoveryService) RunOnce(ctx context.Context) error {
	if s == nil || s.repo == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.cycle.Lock()
	defer s.cycle.Unlock()

	slot := time.Now().UTC().Truncate(quotaRecoveryInterval)
	releaseLeader, acquired := tryAcquireSingletonLeaderLock(
		ctx, s.lockCache, s.db, quotaRecoveryLeaderLockKey, s.instanceID, quotaRecoveryLeaderLockTTL,
	)
	if !acquired {
		return nil
	}
	defer releaseLeader()

	claimed, err := s.repo.ClaimQuotaRecoverySlot(ctx, slot)
	if err != nil || !claimed {
		return err
	}

	cycleCtx, cancel := context.WithTimeout(ctx, quotaRecoveryCycleTimeout)
	defer cancel()
	report, err := s.scanAccounts(cycleCtx)
	if err == nil {
		err = cycleCtx.Err()
	}
	if err != nil {
		return s.releaseFailedSlot(ctx, slot, err)
	}
	slog.Info("quota_recovery_cycle_completed",
		"slot", slot.Format(time.RFC3339),
		"accounts_scanned", report.AccountsScanned,
		"probes_attempted", report.ProbesAttempted,
		"snapshots_saved", report.SnapshotsSaved,
		"blocks_cleared", report.BlocksCleared,
		"errors", report.Errors,
	)
	return nil
}

func (s *QuotaRecoveryService) releaseFailedSlot(parent context.Context, slot time.Time, cycleErr error) error {
	base := context.Background()
	if parent != nil {
		base = context.WithoutCancel(parent)
	}
	ctx, cancel := context.WithTimeout(base, quotaRecoverySlotReleaseTimeout)
	defer cancel()
	released, err := s.repo.ReleaseQuotaRecoverySlot(ctx, slot)
	if err != nil {
		return errors.Join(cycleErr, fmt.Errorf("release quota recovery slot: %w", err))
	}
	if !released {
		slog.Warn("quota_recovery_slot_release_missed", "slot", slot.Format(time.RFC3339))
	}
	return cycleErr
}

func (s *QuotaRecoveryService) scanAccounts(ctx context.Context) (*quotaRecoveryCycleReport, error) {
	report := &quotaRecoveryCycleReport{}
	afterID := int64(0)
	thresholds := map[string]int{}
	if s.settings != nil {
		thresholds = s.settings.GetAccountSchedulingThresholds(ctx)
	}
	for {
		page, err := s.repo.ListQuotaRecoveryAccountPage(ctx, QuotaRecoveryAccountPageOptions{
			AfterID: afterID,
			Limit:   quotaRecoveryPageSize,
		})
		if err != nil {
			return report, err
		}
		if page == nil {
			return report, nil
		}
		report.AccountsScanned += len(page.Accounts)
		s.processPage(ctx, page.Accounts, thresholds, report)
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if !page.HasMore || page.NextAfterID <= afterID {
			return report, nil
		}
		afterID = page.NextAfterID
	}
}

func (s *QuotaRecoveryService) processPage(ctx context.Context, accounts []Account, thresholds map[string]int, report *quotaRecoveryCycleReport) {
	var mu sync.Mutex
	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(quotaRecoveryConcurrency)
	for i := range accounts {
		account := snapshotQuotaRecoveryAccount(&accounts[i])
		if !quotaRecoveryProbeEligible(account) {
			continue
		}
		g.Go(func() error {
			result := s.processAccount(groupCtx, account, thresholds)
			mu.Lock()
			report.ProbesAttempted += result.probes
			report.SnapshotsSaved += result.snapshots
			report.BlocksCleared += result.cleared
			if result.err != nil {
				report.Errors++
			}
			mu.Unlock()
			if result.err != nil && !errors.Is(result.err, context.Canceled) {
				slog.Warn("quota_recovery_account_failed", "account_id", account.ID, "platform", account.Platform, "error", result.err)
			}
			return nil
		})
	}
	_ = g.Wait()
}

func (s *QuotaRecoveryService) processAccount(ctx context.Context, account *Account, thresholds map[string]int) quotaRecoveryAccountResult {
	if account == nil {
		return quotaRecoveryAccountResult{}
	}
	switch {
	case account.Platform == PlatformAnthropic && account.Type == AccountTypeOAuth:
		return s.refreshAnthropic(ctx, account, thresholds)
	case account.Platform == PlatformOpenAI && account.Type == AccountTypeOAuth:
		return s.refreshOpenAI(ctx, account, thresholds)
	case account.Platform == PlatformAntigravity && account.Type == AccountTypeOAuth:
		return s.refreshAntigravity(ctx, account)
	case account.Platform == PlatformGrok && account.Type == AccountTypeOAuth:
		return s.refreshGrok(ctx, account, thresholds)
	default:
		return s.refreshConnectivity(ctx, account)
	}
}

func (s *QuotaRecoveryService) refreshAnthropic(ctx context.Context, account *Account, thresholds map[string]int) quotaRecoveryAccountResult {
	ctx = s.captureRuntimeBlock(ctx, account)
	result := quotaRecoveryAccountResult{probes: 1}
	if s.usageService == nil || s.usageService.usageFetcher == nil {
		result.err = errors.New("Anthropic usage fetcher is not configured")
		return result
	}
	resp, err := s.usageService.fetchOAuthUsageRaw(ctx, account)
	if err != nil {
		result.err = err
		return result
	}
	now := time.Now().UTC()
	usage := s.usageService.buildUsageInfo(resp, &now)
	updates, sessionEnd := buildAnthropicQuotaRecoveryUpdates(usage, now)
	mutation := newQuotaRecoveryMutation(account, account, updates, sessionEnd)

	available := authoritativeAnthropicAvailable(resp, usage)
	if available {
		addObservedAccountBlocks(&mutation, account, refreshedQuotaRecoveryAccount(account, updates, sessionEnd), thresholds, false)
		addObservedQuotaError(&mutation, account)
	}
	if authoritativeAnthropicFableAvailable(resp, usage) {
		addExpectedModelLimit(&mutation, account, anthropicFableRateLimitKey, anthropicFableWindowReason)
	}
	applied := s.applyMutation(ctx, mutation)
	applied.probes = result.probes
	return applied
}

func buildAnthropicQuotaRecoveryUpdates(usage *UsageInfo, now time.Time) (map[string]any, *time.Time) {
	updates := make(map[string]any)
	var sessionEnd *time.Time
	if usage == nil {
		return updates, nil
	}
	if usage.FiveHour != nil {
		updates["session_window_utilization"] = usage.FiveHour.Utilization / 100
		if usage.FiveHour.ResetsAt != nil {
			end := usage.FiveHour.ResetsAt.UTC()
			sessionEnd = &end
		}
	}
	if usage.SevenDay != nil {
		updates["passive_usage_7d_utilization"] = usage.SevenDay.Utilization / 100
		if usage.SevenDay.ResetsAt != nil {
			updates["passive_usage_7d_reset"] = usage.SevenDay.ResetsAt.Unix()
		}
	}
	if usage.SevenDayFable != nil {
		updates["passive_usage_7d_oi_utilization"] = usage.SevenDayFable.Utilization / 100
		if usage.SevenDayFable.ResetsAt != nil {
			updates["passive_usage_7d_oi_reset"] = usage.SevenDayFable.ResetsAt.Unix()
		}
	}
	if len(updates) > 0 {
		updates["passive_usage_sampled_at"] = now.Format(time.RFC3339)
	}
	return updates, sessionEnd
}

func authoritativeAnthropicAvailable(resp *ClaudeUsageResponse, usage *UsageInfo) bool {
	if resp == nil || usage == nil || usage.FiveHour == nil || usage.SevenDay == nil ||
		strings.TrimSpace(resp.FiveHour.ResetsAt) == "" || strings.TrimSpace(resp.SevenDay.ResetsAt) == "" ||
		usage.FiveHour.ResetsAt == nil || usage.SevenDay.ResetsAt == nil {
		return false
	}
	return usage.FiveHour.Utilization < 100 && usage.SevenDay.Utilization < 100
}

func authoritativeAnthropicFableAvailable(resp *ClaudeUsageResponse, usage *UsageInfo) bool {
	return resp != nil && usage != nil && usage.SevenDayFable != nil && usage.SevenDayFable.ResetsAt != nil &&
		strings.TrimSpace(resp.SevenDayOverageIncluded.ResetsAt) != "" && usage.SevenDayFable.Utilization < 100
}

func (s *QuotaRecoveryService) refreshOpenAI(ctx context.Context, account *Account, thresholds map[string]int) quotaRecoveryAccountResult {
	ctx = s.captureRuntimeBlock(ctx, account)
	result := quotaRecoveryAccountResult{probes: 1}
	if s.usageService == nil || s.usageService.openAIQuotaService == nil {
		result.err = errors.New("OpenAI quota service is not configured")
		return result
	}
	identity := account
	if account.IsShadow() {
		if account.ParentAccountID == nil {
			result.err = errors.New("OpenAI shadow has no parent account")
			return result
		}
		parent, err := s.repo.GetByID(ctx, *account.ParentAccountID)
		if err != nil || parent == nil {
			if err == nil {
				err = errors.New("parent account was not found")
			}
			result.err = fmt.Errorf("load OpenAI shadow parent: %w", err)
			return result
		}
		identity = snapshotQuotaRecoveryAccount(parent)
	}
	usage, err := s.usageService.openAIQuotaService.QueryUsage(ctx, account.ID)
	if err != nil {
		result.err = err
		return result
	}
	now := time.Now().UTC()
	limit := openAIQuotaLimitForRecovery(account, usage)
	updates := openAIQuotaRecoveryUpdates(account, usage, limit, now)
	mutation := newQuotaRecoveryMutation(account, identity, updates, nil)
	identityAvailable := quotaRecoveryIdentityAllowsRestore(account, identity)
	if authoritativeOpenAIAvailable(limit) && identityAvailable {
		addObservedAccountBlocks(&mutation, account, refreshedQuotaRecoveryAccount(account, updates, nil), thresholds, false)
		addObservedQuotaError(&mutation, account)
	}
	applied := s.applyMutation(ctx, mutation)
	applied.probes = result.probes
	return applied
}

func openAIQuotaLimitForRecovery(account *Account, usage *OpenAIQuotaUsage) *OpenAIRateLimit {
	if account == nil || usage == nil {
		return nil
	}
	if account.QuotaDimensionOrDefault() != QuotaDimensionSpark {
		return usage.RateLimit
	}
	for i := range usage.AdditionalRateLimits {
		item := &usage.AdditionalRateLimits[i]
		if strings.EqualFold(strings.TrimSpace(item.MeteredFeature), "codex_bengalfox") {
			return item.RateLimit
		}
	}
	return nil
}

func authoritativeOpenAIAvailable(limit *OpenAIRateLimit) bool {
	if limit == nil || (limit.PrimaryWindow == nil && limit.SecondaryWindow == nil) || !limit.Allowed || limit.LimitReached {
		return false
	}
	for _, window := range []*OpenAIRateLimitWindow{limit.PrimaryWindow, limit.SecondaryWindow} {
		if window != nil && window.UsedPercent >= 100 {
			return false
		}
	}
	return true
}

func openAIQuotaRecoveryUpdates(account *Account, usage *OpenAIQuotaUsage, limit *OpenAIRateLimit, now time.Time) map[string]any {
	var updates map[string]any
	if account != nil && account.QuotaDimensionOrDefault() == QuotaDimensionSpark {
		updates = buildCodexSparkWindowExtraUpdates(usage, now)
	} else {
		updates = buildCodexExtraUpdatesFromRateLimit(limit, now)
	}
	if usage != nil && usage.RateLimitResetCredits != nil &&
		(usage.RateLimitResetCredits.AvailableCount == 0 || len(usage.RateLimitResetCredits.Credits) > 0) {
		if updates == nil {
			updates = make(map[string]any)
		}
		updates[openaiQuotaResetCreditsKey] = usage.RateLimitResetCredits
	}
	return updates
}

func buildCodexExtraUpdatesFromRateLimit(limit *OpenAIRateLimit, now time.Time) map[string]any {
	if limit == nil {
		return nil
	}
	snapshot := &OpenAICodexUsageSnapshot{}
	if window := limit.PrimaryWindow; window != nil {
		used, reset, minutes := window.UsedPercent, int(window.ResetAfterSeconds), int(window.LimitWindowSeconds/60)
		snapshot.PrimaryUsedPercent = &used
		snapshot.PrimaryResetAfterSeconds = &reset
		snapshot.PrimaryWindowMinutes = &minutes
	}
	if window := limit.SecondaryWindow; window != nil {
		used, reset, minutes := window.UsedPercent, int(window.ResetAfterSeconds), int(window.LimitWindowSeconds/60)
		snapshot.SecondaryUsedPercent = &used
		snapshot.SecondaryResetAfterSeconds = &reset
		snapshot.SecondaryWindowMinutes = &minutes
	}
	return buildCodexUsageExtraUpdates(snapshot, now)
}

func (s *QuotaRecoveryService) refreshAntigravity(ctx context.Context, account *Account) quotaRecoveryAccountResult {
	ctx = s.captureRuntimeBlock(ctx, account)
	result := quotaRecoveryAccountResult{probes: 1}
	if s.usageService == nil || s.usageService.antigravityQuotaFetcher == nil ||
		!s.usageService.antigravityQuotaFetcher.CanFetch(account) {
		result.err = errors.New("Antigravity quota fetcher is not configured")
		return result
	}
	proxyURL := s.usageService.antigravityQuotaFetcher.GetProxyURL(ctx, account)
	fetched, err := s.usageService.antigravityQuotaFetcher.FetchQuota(ctx, account, proxyURL)
	if err != nil {
		result.err = err
		return result
	}
	if fetched == nil || fetched.UsageInfo == nil {
		result.err = errors.New("Antigravity returned no quota snapshot")
		return result
	}
	updates := map[string]any{QuotaRecoveryAntigravityUsageSnapshotExtraKey: fetched.UsageInfo}
	mutation := newQuotaRecoveryMutation(account, account, updates, nil)
	if authoritativeAntigravityAvailable(account, fetched.UsageInfo) {
		addObservedGlobalRateLimit(&mutation, account)
		addObservedQuotaError(&mutation, account)
	}
	addRecoverableAntigravityModelLimits(&mutation, account, fetched.UsageInfo)
	applied := s.applyMutation(ctx, mutation)
	applied.probes = result.probes
	if applied.err == nil && applied.snapshots > 0 && s.usageService.cache != nil {
		s.usageService.cache.antigravityCache.Store(account.ID, &antigravityUsageCache{
			usageInfo: fetched.UsageInfo,
			timestamp: time.Now(),
		})
	}
	return applied
}

func authoritativeAntigravityAvailable(account *Account, usage *UsageInfo) bool {
	if usage == nil || usage.IsForbidden || usage.NeedsReauth || usage.ErrorCode != "" || usage.Error != "" {
		return false
	}
	// Antigravity quotas are model-scoped. One available model is enough to
	// prove that a coarse account-level block is stale; exhausted models remain
	// protected by their own model_rate_limits entries.
	for _, quota := range usage.AntigravityQuota {
		if quota != nil && quota.Utilization < 100 {
			return true
		}
	}
	// AI Credits are a real additional call path only when the operator enabled
	// overages for this account.
	if account != nil && account.IsOveragesEnabled() {
		for _, credit := range usage.AICredits {
			if credit.Amount > credit.MinimumBalance {
				return true
			}
		}
	}
	return false
}

func addRecoverableAntigravityModelLimits(mutation *QuotaRecoveryMutation, account *Account, usage *UsageInfo) {
	if mutation == nil || account == nil || usage == nil || usage.IsForbidden || usage.NeedsReauth || usage.ErrorCode != "" || usage.Error != "" {
		return
	}
	limits := modelRateLimitMap(account)
	if len(limits) == 0 {
		return
	}
	quotaByName := make(map[string]*AntigravityModelQuota, len(usage.AntigravityQuota))
	for name, quota := range usage.AntigravityQuota {
		quotaByName[strings.ToLower(strings.TrimSpace(name))] = quota
	}
	geminiObserved, geminiAvailable := false, true
	for name, quota := range quotaByName {
		if strings.HasPrefix(normalizeAntigravityModelName(name), "gemini-") && quota != nil {
			geminiObserved = true
			if quota.Utilization >= 100 {
				geminiAvailable = false
			}
		}
	}
	creditsAvailable := false
	for _, credit := range usage.AICredits {
		if credit.Amount > credit.MinimumBalance {
			creditsAvailable = true
			break
		}
	}
	keys := make([]string, 0, len(limits))
	for key, raw := range limits {
		if modelRateLimitReason(raw) != "" {
			continue
		}
		switch key {
		case antigravityGeminiModelRateLimitKey:
			if geminiObserved && geminiAvailable {
				keys = append(keys, key)
			}
		case creditsExhaustedKey:
			if creditsAvailable {
				keys = append(keys, key)
			}
		default:
			if quota := quotaByName[strings.ToLower(strings.TrimSpace(key))]; quota != nil && quota.Utilization < 100 {
				keys = append(keys, key)
			}
		}
	}
	for _, key := range keys {
		addExpectedModelLimit(mutation, account, key, "")
	}
}

func (s *QuotaRecoveryService) refreshGrok(ctx context.Context, account *Account, thresholds map[string]int) quotaRecoveryAccountResult {
	ctx = s.captureRuntimeBlock(ctx, account)
	result := quotaRecoveryAccountResult{probes: 1}
	if s.usageService == nil || s.usageService.grokQuotaService == nil {
		result.err = errors.New("Grok quota service is not configured")
		return result
	}
	billing, err := s.usageService.grokQuotaService.ProbeBillingForQuotaRecovery(ctx, account.ID)
	if err != nil {
		result.err = err
	}
	if billing != nil && billing.Billing != nil {
		display := newQuotaRecoveryMutation(account, account, map[string]any{grokBillingExtraKey: billing.Billing}, nil)
		applied := s.applyMutation(ctx, display)
		result.snapshots += applied.snapshots
		result.cleared += applied.cleared
		if applied.err != nil {
			result.err = applied.err
			return result
		}
	}

	hadGlobal := account.RateLimitedAt != nil && account.RateLimitResetAt != nil
	hadThreshold := account.TempUnschedulableUntil != nil && IsAccountSchedulingThresholdReason(account.TempUnschedulableReason)
	hadQuotaError := hasQuotaRecoveryBalanceError(account)
	if (!account.Schedulable && !hadQuotaError) || (!hadGlobal && !hadThreshold && !hadQuotaError) {
		return result
	}
	result.probes++
	active, err := s.usageService.grokQuotaService.ProbeUsageForQuotaRecovery(ctx, account.ID)
	if err != nil || !grokActiveProbeAvailable(active) {
		if err != nil {
			result.err = err
		}
		return result
	}
	latest, err := s.repo.GetByID(ctx, account.ID)
	if err != nil || latest == nil {
		if err == nil {
			err = errors.New("Grok account was not found after quota probe")
		}
		result.err = err
		return result
	}
	updates := map[string]any(nil)
	if active.Snapshot != nil {
		updates = map[string]any{grokQuotaSnapshotExtraKey: active.Snapshot}
		for key, value := range buildGrokSchedulerExtraUpdates(active.Snapshot) {
			updates[key] = value
		}
	}
	mutation := newQuotaRecoveryMutation(account, account, updates, nil)
	if hadGlobal && sameObservedGlobalRateLimit(account, latest) {
		addObservedGlobalRateLimit(&mutation, account)
	}
	refreshed := refreshedQuotaRecoveryAccount(latest, updates, nil)
	if hadThreshold && sameObservedThresholdBlock(account, latest) && refreshed != nil &&
		!EvaluateAccountSchedulingThreshold(refreshed, thresholds, time.Now().UTC()).ShouldPause {
		addObservedThresholdBlock(&mutation, account)
	}
	if hadQuotaError && hasSameQuotaRecoveryBalanceError(account, latest) {
		addObservedQuotaError(&mutation, account)
	}
	applied := s.applyMutation(ctx, mutation)
	result.snapshots += applied.snapshots
	result.cleared += applied.cleared
	if applied.err != nil {
		result.err = applied.err
	}
	return result
}

func grokActiveProbeAvailable(result *GrokQuotaProbeResult) bool {
	return result != nil && result.StatusCode >= http.StatusOK && result.StatusCode < http.StatusMultipleChoices
}

func (s *QuotaRecoveryService) refreshConnectivity(ctx context.Context, account *Account) quotaRecoveryAccountResult {
	ctx = s.captureRuntimeBlock(ctx, account)
	if !quotaRecoveryConnectivityProbeEligible(account) {
		return quotaRecoveryAccountResult{}
	}
	result := quotaRecoveryAccountResult{probes: 1}
	if s.accountTest == nil {
		result.err = errors.New("account test service is not configured")
		return result
	}
	probeCtx, cancel := context.WithTimeout(withQuotaRecoveryReadOnlyProbe(ctx), 30*time.Second)
	defer cancel()
	testResult, err := s.accountTest.RunTestBackground(probeCtx, account.ID, "")
	if err != nil || testResult == nil || testResult.Status != "success" {
		if err != nil {
			result.err = err
		} else if testResult != nil && strings.TrimSpace(testResult.ErrorMessage) != "" {
			result.err = errors.New(testResult.ErrorMessage)
		}
		return result
	}
	mutation := newQuotaRecoveryMutation(account, account, nil, nil)
	addObservedGlobalRateLimit(&mutation, account)
	addObservedThresholdBlock(&mutation, account)
	addObservedQuotaError(&mutation, account)
	applied := s.applyMutation(ctx, mutation)
	applied.probes = result.probes
	return applied
}

func (s *QuotaRecoveryService) applyMutation(ctx context.Context, mutation QuotaRecoveryMutation) quotaRecoveryAccountResult {
	if mutation.Target == nil || mutation.Identity == nil ||
		(len(mutation.ExtraUpdates) == 0 && mutation.SessionWindowEnd == nil && !mutation.clearsSchedulingBlock()) {
		return quotaRecoveryAccountResult{}
	}
	applied, err := s.repo.ApplyQuotaRecoveryMutation(ctx, mutation)
	result := quotaRecoveryAccountResult{err: err}
	if err != nil || !applied {
		return result
	}
	if len(mutation.ExtraUpdates) > 0 || mutation.SessionWindowEnd != nil {
		result.snapshots = 1
	}
	if mutation.clearsSchedulingBlock() {
		result.cleared = 1
	}
	if mutation.ClearThresholdBlock {
		s.clearObservedTempUnschedCache(ctx, mutation)
	}
	if mutation.ClearGlobalRateLimit || mutation.ClearThresholdBlock || mutation.ClearQuotaError {
		s.clearObservedRuntimeBlock(ctx, mutation)
	}
	return result
}

func (s *QuotaRecoveryService) captureRuntimeBlock(ctx context.Context, account *Account) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || account == nil || account.ID <= 0 {
		return ctx
	}
	if observed, ok := ctx.Value(quotaRecoveryRuntimeBlockContextKey{}).(quotaRecoveryRuntimeBlockObservation); ok &&
		observed.accountID == account.ID {
		return ctx
	}
	blocker, ok := s.runtimeBlocker.(QuotaRecoveryRuntimeBlocker)
	if !ok || blocker == nil {
		return ctx
	}
	snapshot, present := blocker.SnapshotQuotaRecoveryRuntimeBlock(account.ID)
	return context.WithValue(ctx, quotaRecoveryRuntimeBlockContextKey{}, quotaRecoveryRuntimeBlockObservation{
		accountID: account.ID,
		snapshot:  snapshot,
		present:   present,
	})
}

func (s *QuotaRecoveryService) clearObservedRuntimeBlock(ctx context.Context, mutation QuotaRecoveryMutation) {
	if s == nil || mutation.Target == nil || ctx == nil {
		return
	}
	blocker, ok := s.runtimeBlocker.(QuotaRecoveryRuntimeBlocker)
	if !ok || blocker == nil {
		return
	}
	observed, ok := ctx.Value(quotaRecoveryRuntimeBlockContextKey{}).(quotaRecoveryRuntimeBlockObservation)
	if !ok || !observed.present || observed.accountID != mutation.Target.ID {
		return
	}
	blocker.ClearQuotaRecoveryRuntimeBlock(
		mutation.Target.ID,
		observed.snapshot,
		mutation.ClearGlobalRateLimit,
		mutation.ClearThresholdBlock,
		mutation.ClearQuotaError,
	)
}

func (s *QuotaRecoveryService) clearObservedTempUnschedCache(ctx context.Context, mutation QuotaRecoveryMutation) {
	if s == nil || mutation.Target == nil || mutation.ExpectedTempUntil == nil || mutation.ExpectedTempReason == "" {
		return
	}
	cache, ok := s.tempCache.(QuotaRecoveryTempUnschedCache)
	if !ok || cache == nil {
		return
	}
	expected := tempUnschedStateFromStoredReason(mutation.ExpectedTempReason, mutation.ExpectedTempUntil.Unix())
	if expected == nil {
		return
	}
	if _, err := cache.DeleteTempUnschedIfMatch(ctx, mutation.Target.ID, expected); err != nil {
		slog.Warn("quota_recovery_temp_cache_conditional_delete_failed", "account_id", mutation.Target.ID, "error", err)
	}
}

func newQuotaRecoveryMutation(target, identity *Account, updates map[string]any, sessionEnd *time.Time) QuotaRecoveryMutation {
	return QuotaRecoveryMutation{
		Target:           snapshotQuotaRecoveryAccount(target),
		Identity:         snapshotQuotaRecoveryAccount(identity),
		ExtraUpdates:     cloneQuotaRecoveryJSONMap(updates),
		SessionWindowEnd: cloneTimePtr(sessionEnd),
	}
}

func addObservedAccountBlocks(mutation *QuotaRecoveryMutation, observed, refreshed *Account, thresholds map[string]int, clearThresholdWithoutUsage bool) {
	if mutation == nil || !quotaRecoveryCanClearObservedBlocks(observed) {
		return
	}
	addObservedGlobalRateLimit(mutation, observed)
	if observed.TempUnschedulableUntil == nil || !IsAccountSchedulingThresholdReason(observed.TempUnschedulableReason) {
		return
	}
	if clearThresholdWithoutUsage || refreshed == nil ||
		!EvaluateAccountSchedulingThreshold(refreshed, thresholds, time.Now().UTC()).ShouldPause {
		addObservedThresholdBlock(mutation, observed)
	}
}

func addObservedGlobalRateLimit(mutation *QuotaRecoveryMutation, account *Account) {
	if mutation == nil || !quotaRecoveryCanClearObservedBlocks(account) || account.RateLimitedAt == nil || account.RateLimitResetAt == nil {
		return
	}
	mutation.ClearGlobalRateLimit = true
	mutation.ExpectedRateLimitedAt = cloneTimePtr(account.RateLimitedAt)
	mutation.ExpectedRateLimitResetAt = cloneTimePtr(account.RateLimitResetAt)
}

func addObservedThresholdBlock(mutation *QuotaRecoveryMutation, account *Account) {
	if mutation == nil || !quotaRecoveryCanClearObservedBlocks(account) || account.TempUnschedulableUntil == nil ||
		!IsAccountSchedulingThresholdReason(account.TempUnschedulableReason) {
		return
	}
	mutation.ClearThresholdBlock = true
	mutation.ExpectedTempUntil = cloneTimePtr(account.TempUnschedulableUntil)
	mutation.ExpectedTempReason = account.TempUnschedulableReason
}

func addObservedQuotaError(mutation *QuotaRecoveryMutation, account *Account) {
	if mutation == nil || !hasQuotaRecoveryBalanceError(account) {
		return
	}
	mutation.ClearQuotaError = true
}

func addExpectedModelLimit(mutation *QuotaRecoveryMutation, account *Account, key, allowedReason string) {
	if mutation == nil || !quotaRecoveryCanClearObservedBlocks(account) || strings.TrimSpace(key) == "" {
		return
	}
	raw, ok := modelRateLimitMap(account)[key]
	if !ok {
		return
	}
	reason := modelRateLimitReason(raw)
	if reason != "" && reason != allowedReason {
		return
	}
	if mutation.ExpectedModelRateLimits == nil {
		mutation.ExpectedModelRateLimits = make(map[string]any)
	}
	mutation.ExpectedModelRateLimits[key] = cloneQuotaRecoveryJSONValue(raw)
	mutation.ClearModelRateLimitKeys = append(mutation.ClearModelRateLimitKeys, key)
	sort.Strings(mutation.ClearModelRateLimitKeys)
}

func modelRateLimitMap(account *Account) map[string]any {
	if account == nil || len(account.Extra) == 0 {
		return nil
	}
	limits, _ := account.Extra[modelRateLimitsKey].(map[string]any)
	return limits
}

func modelRateLimitReason(raw any) string {
	payload, _ := raw.(map[string]any)
	return strings.TrimSpace(stringValue(payload["reason"]))
}

func hasQuotaDerivedAccountBlock(account *Account) bool {
	return account != nil && account.Schedulable &&
		((account.RateLimitedAt != nil && account.RateLimitResetAt != nil) ||
			(account.TempUnschedulableUntil != nil && IsAccountSchedulingThresholdReason(account.TempUnschedulableReason)))
}

const (
	QuotaRecoveryCreditBalanceErrorPrefix = "Credit balance exhausted (400):"
	QuotaRecoveryPaymentErrorPrefix       = "Payment required (402):"
)

// IsQuotaRecoveryBalanceErrorMessage identifies only the two balance failures
// that RateLimitService persists with SetError. Authentication, entitlement,
// operator and generic custom errors must never be automatically re-enabled.
func IsQuotaRecoveryBalanceErrorMessage(message string) bool {
	message = strings.TrimSpace(message)
	return strings.HasPrefix(message, QuotaRecoveryCreditBalanceErrorPrefix) ||
		strings.HasPrefix(message, QuotaRecoveryPaymentErrorPrefix)
}

func hasQuotaRecoveryBalanceError(account *Account) bool {
	return account != nil && account.Status == StatusError && !account.Schedulable &&
		IsQuotaRecoveryBalanceErrorMessage(account.ErrorMessage)
}

func quotaRecoveryCanClearObservedBlocks(account *Account) bool {
	return account != nil && (account.Schedulable || hasQuotaRecoveryBalanceError(account))
}

func hasSameQuotaRecoveryBalanceError(expected, current *Account) bool {
	return hasQuotaRecoveryBalanceError(expected) && hasQuotaRecoveryBalanceError(current) &&
		expected.ErrorMessage == current.ErrorMessage
}

func quotaRecoveryIdentityAllowsRestore(target, identity *Account) bool {
	if target == nil || identity == nil {
		return false
	}
	if identity.Status == StatusActive && identity.Schedulable {
		return true
	}
	return identity.ID == target.ID && hasQuotaRecoveryBalanceError(identity)
}

func sameObservedGlobalRateLimit(expected, current *Account) bool {
	return expected != nil && current != nil && expected.RateLimitedAt != nil && expected.RateLimitResetAt != nil &&
		current.RateLimitedAt != nil && current.RateLimitResetAt != nil &&
		expected.RateLimitedAt.Equal(*current.RateLimitedAt) && expected.RateLimitResetAt.Equal(*current.RateLimitResetAt)
}

func sameObservedThresholdBlock(expected, current *Account) bool {
	return expected != nil && current != nil && expected.TempUnschedulableUntil != nil && current.TempUnschedulableUntil != nil &&
		expected.TempUnschedulableUntil.Equal(*current.TempUnschedulableUntil) && expected.TempUnschedulableReason == current.TempUnschedulableReason
}

func refreshedQuotaRecoveryAccount(account *Account, updates map[string]any, sessionEnd *time.Time) *Account {
	refreshed := snapshotQuotaRecoveryAccount(account)
	if refreshed == nil {
		return nil
	}
	mergeAccountExtra(refreshed, updates)
	if sessionEnd != nil {
		refreshed.SessionWindowEnd = cloneTimePtr(sessionEnd)
	}
	return refreshed
}

func quotaRecoveryProbeEligible(account *Account) bool {
	return quotaRecoveryAuthoritativeProbeEligible(account) || quotaRecoveryConnectivityProbeEligible(account)
}

func quotaRecoveryAccountBaseEligible(account *Account) bool {
	if account == nil || account.ID <= 0 ||
		(account.Status != StatusActive && !hasQuotaRecoveryBalanceError(account)) || account.IsSyntheticUITest() {
		return false
	}
	if account.AutoPauseOnExpired && account.ExpiresAt != nil && !time.Now().Before(*account.ExpiresAt) {
		return false
	}
	return true
}

// quotaRecoveryAuthoritativeProbeEligible identifies the credential types for
// which sub2api has a provider-specific quota/balance endpoint. Setup Tokens
// are deliberately excluded: their inference-only scope cannot call the
// Anthropic profile usage endpoint.
func quotaRecoveryAuthoritativeProbeEligible(account *Account) bool {
	if !quotaRecoveryAccountBaseEligible(account) {
		return false
	}
	switch account.Platform {
	case PlatformAnthropic:
		return account.Type == AccountTypeOAuth && strings.TrimSpace(account.GetCredential("access_token")) != ""
	case PlatformOpenAI:
		return account.Type == AccountTypeOAuth
	case PlatformAntigravity:
		return account.Type == AccountTypeOAuth && strings.TrimSpace(account.GetCredential("access_token")) != ""
	case PlatformGrok:
		return account.Type == AccountTypeOAuth
	default:
		return false
	}
}

// quotaRecoveryConnectivityProbeEligible covers accounts whose supported
// credential type has no authoritative balance endpoint. Active accounts must
// remain manually schedulable; the only exception is a fixed balance error
// written by RateLimitService, which a successful inference probe may recover.
// Connectivity probes never invent a balance snapshot.
func quotaRecoveryConnectivityProbeEligible(account *Account) bool {
	if !quotaRecoveryAccountBaseEligible(account) ||
		(!hasQuotaDerivedAccountBlock(account) && !hasQuotaRecoveryBalanceError(account)) ||
		!quotaRecoveryConnectivityTypeSupported(account) {
		return false
	}
	switch account.Type {
	case AccountTypeAPIKey:
		return strings.TrimSpace(account.GetCredential("api_key")) != ""
	case AccountTypeSetupToken:
		return strings.TrimSpace(account.GetCredential("access_token")) != ""
	case AccountTypeUpstream:
		return strings.TrimSpace(account.GetCredential("api_key")) != "" &&
			strings.TrimSpace(account.GetCredential("base_url")) != ""
	case AccountTypeBedrock:
		if account.IsBedrockAPIKey() {
			return strings.TrimSpace(account.GetCredential("api_key")) != ""
		}
		return strings.TrimSpace(account.GetCredential("aws_access_key_id")) != "" &&
			strings.TrimSpace(account.GetCredential("aws_secret_access_key")) != ""
	case AccountTypeServiceAccount:
		_, err := parseVertexServiceAccountKey(account)
		return err == nil
	case AccountTypeOAuth:
		return strings.TrimSpace(account.GetCredential("access_token")) != "" ||
			strings.TrimSpace(account.GetCredential("refresh_token")) != ""
	default:
		return false
	}
}

func quotaRecoveryConnectivityTypeSupported(account *Account) bool {
	if account == nil {
		return false
	}
	switch account.Platform {
	case PlatformAnthropic:
		return account.Type == AccountTypeSetupToken || account.Type == AccountTypeAPIKey ||
			account.Type == AccountTypeBedrock || account.Type == AccountTypeServiceAccount
	case PlatformOpenAI:
		return account.Type == AccountTypeAPIKey
	case PlatformGemini:
		return account.Type == AccountTypeOAuth || account.Type == AccountTypeAPIKey ||
			account.Type == AccountTypeServiceAccount
	case PlatformAntigravity:
		return account.Type == AccountTypeAPIKey || account.Type == AccountTypeUpstream
	case PlatformGrok:
		return account.Type == AccountTypeAPIKey
	default:
		return false
	}
}

func snapshotQuotaRecoveryAccount(account *Account) *Account {
	if account == nil {
		return nil
	}
	snapshot := *account
	snapshot.Credentials = cloneQuotaRecoveryJSONMap(account.Credentials)
	snapshot.Extra = cloneQuotaRecoveryJSONMap(account.Extra)
	snapshot.ProxyID = cloneQuotaRecoveryInt64Ptr(account.ProxyID)
	snapshot.ParentAccountID = cloneQuotaRecoveryInt64Ptr(account.ParentAccountID)
	snapshot.ExpiresAt = cloneTimePtr(account.ExpiresAt)
	snapshot.RateLimitedAt = cloneTimePtr(account.RateLimitedAt)
	snapshot.RateLimitResetAt = cloneTimePtr(account.RateLimitResetAt)
	snapshot.TempUnschedulableUntil = cloneTimePtr(account.TempUnschedulableUntil)
	if account.Proxy != nil {
		proxy := *account.Proxy
		snapshot.Proxy = &proxy
	}
	return &snapshot
}

func cloneQuotaRecoveryInt64Ptr(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneQuotaRecoveryJSONMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return nil
	}
	raw, err := json.Marshal(src)
	if err == nil {
		var cloned map[string]any
		if json.Unmarshal(raw, &cloned) == nil {
			return cloned
		}
	}
	cloned := make(map[string]any, len(src))
	for key, value := range src {
		cloned[key] = value
	}
	return cloned
}

func cloneQuotaRecoveryJSONValue(src any) any {
	raw, err := json.Marshal(src)
	if err != nil {
		return src
	}
	var cloned any
	if json.Unmarshal(raw, &cloned) != nil {
		return src
	}
	return cloned
}

type quotaRecoveryReadOnlyProbeKey struct{}

func withQuotaRecoveryReadOnlyProbe(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, quotaRecoveryReadOnlyProbeKey{}, true)
}

func isQuotaRecoveryReadOnlyProbe(ctx context.Context) bool {
	enabled, _ := ctx.Value(quotaRecoveryReadOnlyProbeKey{}).(bool)
	return enabled
}

func antigravityUsageSnapshotFromExtra(account *Account) *UsageInfo {
	if account == nil || len(account.Extra) == 0 {
		return nil
	}
	raw := account.Extra[QuotaRecoveryAntigravityUsageSnapshotExtraKey]
	if raw == nil {
		return nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var usage UsageInfo
	if json.Unmarshal(encoded, &usage) != nil || (len(usage.AntigravityQuota) == 0 && len(usage.AICredits) == 0 && !usage.IsForbidden) {
		return nil
	}
	return &usage
}
