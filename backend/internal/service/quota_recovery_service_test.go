//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai_compat"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type quotaRecoveryRepositoryStub struct {
	AccountRepository

	mu sync.Mutex

	accounts map[int64]*Account
	claimed  *time.Time

	claimSlots        []time.Time
	releaseSlots      []time.Time
	releaseContextErr []error
	listCalls         int
	listErr           error
	listHook          func()
	page              *QuotaRecoveryAccountPage

	applyResult    bool
	applyResultSet bool
	applyErr       error
	mutations      []QuotaRecoveryMutation
	firstClaim     chan struct{}
}

func (r *quotaRecoveryRepositoryStub) GetByID(_ context.Context, id int64) (*Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	account := r.accounts[id]
	if account == nil {
		return nil, ErrAccountNotFound
	}
	return snapshotQuotaRecoveryAccount(account), nil
}

func (r *quotaRecoveryRepositoryStub) ClaimQuotaRecoverySlot(_ context.Context, slot time.Time) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.claimSlots = append(r.claimSlots, slot)
	if len(r.claimSlots) == 1 && r.firstClaim != nil {
		close(r.firstClaim)
	}
	if r.claimed != nil {
		return false, nil
	}
	claimed := slot.UTC()
	r.claimed = &claimed
	return true, nil
}

func (r *quotaRecoveryRepositoryStub) ReleaseQuotaRecoverySlot(ctx context.Context, slot time.Time) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.releaseSlots = append(r.releaseSlots, slot)
	r.releaseContextErr = append(r.releaseContextErr, ctx.Err())
	if r.claimed == nil || !r.claimed.Equal(slot) {
		return false, nil
	}
	r.claimed = nil
	return true, nil
}

func (r *quotaRecoveryRepositoryStub) ListQuotaRecoveryAccountPage(context.Context, QuotaRecoveryAccountPageOptions) (*QuotaRecoveryAccountPage, error) {
	r.mu.Lock()
	r.listCalls++
	hook := r.listHook
	r.listHook = nil
	err := r.listErr
	r.listErr = nil
	page := r.page
	r.mu.Unlock()
	if hook != nil {
		hook()
	}
	if err != nil {
		return nil, err
	}
	if page == nil {
		return &QuotaRecoveryAccountPage{}, nil
	}
	return page, nil
}

func (r *quotaRecoveryRepositoryStub) ApplyQuotaRecoveryMutation(_ context.Context, mutation QuotaRecoveryMutation) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mutations = append(r.mutations, mutation)
	if r.applyErr != nil {
		return false, r.applyErr
	}
	if r.applyResultSet {
		return r.applyResult, nil
	}
	return true, nil
}

func (r *quotaRecoveryRepositoryStub) snapshot() (claimCount, releaseCount, listCalls int, mutations []QuotaRecoveryMutation, releaseErrors []error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.claimSlots), len(r.releaseSlots), r.listCalls,
		append([]QuotaRecoveryMutation(nil), r.mutations...), append([]error(nil), r.releaseContextErr...)
}

type quotaRecoveryAccountTesterStub struct {
	result   *ScheduledTestResult
	err      error
	calls    int
	account  int64
	readOnly bool
	hook     func()
}

func (s *quotaRecoveryAccountTesterStub) RunTestBackground(ctx context.Context, accountID int64, _ string) (*ScheduledTestResult, error) {
	s.calls++
	s.account = accountID
	s.readOnly = isQuotaRecoveryReadOnlyProbe(ctx)
	if s.hook != nil {
		s.hook()
	}
	return s.result, s.err
}

type quotaRecoveryTempCacheStub struct {
	deleted      []int64
	broadDeleted []int64
	expected     []*TempUnschedState
	deleteErr    error
}

func (*quotaRecoveryTempCacheStub) SetTempUnsched(context.Context, int64, *TempUnschedState) error {
	return nil
}

func (*quotaRecoveryTempCacheStub) GetTempUnsched(context.Context, int64) (*TempUnschedState, error) {
	return nil, nil
}

func (s *quotaRecoveryTempCacheStub) DeleteTempUnsched(_ context.Context, accountID int64) error {
	s.broadDeleted = append(s.broadDeleted, accountID)
	return nil
}

func (s *quotaRecoveryTempCacheStub) DeleteTempUnschedIfMatch(_ context.Context, accountID int64, expected *TempUnschedState) (bool, error) {
	s.deleted = append(s.deleted, accountID)
	s.expected = append(s.expected, expected)
	if s.deleteErr != nil {
		return false, s.deleteErr
	}
	return true, nil
}

type quotaRecoveryRuntimeBlockerStub struct {
	cleared          []int64
	broadCleared     []int64
	clearQuotaErrors []bool
	snapshot         QuotaRecoveryRuntimeBlockSnapshot
	present          bool
}

func (*quotaRecoveryRuntimeBlockerStub) BlockAccountScheduling(*Account, time.Time, string) {}

func (s *quotaRecoveryRuntimeBlockerStub) ClearAccountSchedulingBlock(accountID int64) {
	s.broadCleared = append(s.broadCleared, accountID)
}

func (s *quotaRecoveryRuntimeBlockerStub) SnapshotQuotaRecoveryRuntimeBlock(_ int64) (QuotaRecoveryRuntimeBlockSnapshot, bool) {
	if s.snapshot.Generation == 0 {
		s.snapshot = QuotaRecoveryRuntimeBlockSnapshot{
			Generation: 1,
			Until:      time.Now().Add(time.Hour),
			Reason:     "account_scheduling_threshold",
		}
		return s.snapshot, true
	}
	return s.snapshot, s.present
}

func (s *quotaRecoveryRuntimeBlockerStub) ClearQuotaRecoveryRuntimeBlock(
	accountID int64,
	_ QuotaRecoveryRuntimeBlockSnapshot,
	_ bool,
	_ bool,
	clearQuotaError bool,
) bool {
	s.cleared = append(s.cleared, accountID)
	s.clearQuotaErrors = append(s.clearQuotaErrors, clearQuotaError)
	return true
}

type quotaRecoveryClaudeFetcherStub struct {
	response *ClaudeUsageResponse
	err      error
}

func (s *quotaRecoveryClaudeFetcherStub) FetchUsage(context.Context, string, string) (*ClaudeUsageResponse, error) {
	return s.response, s.err
}

func (s *quotaRecoveryClaudeFetcherStub) FetchUsageWithOptions(context.Context, *ClaudeUsageFetchOptions) (*ClaudeUsageResponse, error) {
	return s.response, s.err
}

func TestQuotaRecoveryRunLoopWaitAlignsToUTCFourHourSlots(t *testing.T) {
	t.Parallel()

	shanghai := time.FixedZone("Asia/Shanghai", 8*60*60)
	now := time.Date(2026, 8, 14, 9, 30, 0, 0, shanghai)
	require.Equal(t, 2*time.Hour+30*time.Minute, quotaRecoveryRunLoopWait(now, nil))
	require.Equal(t, quotaRecoveryFailureRetryInterval, quotaRecoveryRunLoopWait(now, errors.New("scan failed")))

	exactBoundary := time.Date(2026, 8, 14, 4, 0, 0, 0, time.UTC)
	require.Equal(t, quotaRecoveryInterval, quotaRecoveryRunLoopWait(exactBoundary, nil))
}

func TestQuotaRecoveryRunOnceReleasesFailedSlotWithDetachedContext(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("page query failed")
	ctx, cancel := context.WithCancel(context.Background())
	repo := &quotaRecoveryRepositoryStub{
		listErr:  sentinel,
		listHook: cancel,
	}
	svc := NewQuotaRecoveryService(repo, nil, nil, nil, nil, nil, nil, nil)

	err := svc.RunOnce(ctx)
	require.ErrorIs(t, err, sentinel)
	claimCount, releaseCount, listCalls, _, releaseErrors := repo.snapshot()
	require.Equal(t, 1, claimCount)
	require.Equal(t, 1, releaseCount)
	require.Equal(t, 1, listCalls)
	require.Len(t, releaseErrors, 1)
	require.NoError(t, releaseErrors[0])

	// The failed slot is retryable once. A successful retry retains the claim,
	// so another invocation in the same slot is deduplicated.
	require.NoError(t, svc.RunOnce(context.Background()))
	require.NoError(t, svc.RunOnce(context.Background()))
	claimCount, releaseCount, listCalls, _, _ = repo.snapshot()
	require.Equal(t, 3, claimCount)
	require.Equal(t, 1, releaseCount)
	require.Equal(t, 2, listCalls)
}

func TestQuotaRecoveryStartStopAreIdempotent(t *testing.T) {
	t.Parallel()

	repo := &quotaRecoveryRepositoryStub{firstClaim: make(chan struct{})}
	svc := NewQuotaRecoveryService(repo, nil, nil, nil, nil, nil, nil, nil)
	svc.Start()
	svc.Start()

	select {
	case <-repo.firstClaim:
	case <-time.After(2 * time.Second):
		t.Fatal("quota recovery did not run its startup cycle")
	}
	svc.Stop()
	svc.Stop()
	svc.Start()

	claimCount, _, listCalls, _, _ := repo.snapshot()
	require.Equal(t, 1, claimCount)
	require.Equal(t, 1, listCalls)
}

func TestQuotaRecoveryAPIKeySuccessClearsOnlyObservedQuotaBlocks(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	resetAt := now.Add(time.Hour)
	tempUntil := now.Add(2 * time.Hour)
	account := &Account{
		ID:                      41,
		Platform:                PlatformOpenAI,
		Type:                    AccountTypeAPIKey,
		Status:                  StatusActive,
		Schedulable:             true,
		Credentials:             map[string]any{"api_key": "sk-test"},
		RateLimitedAt:           &now,
		RateLimitResetAt:        &resetAt,
		TempUnschedulableUntil:  &tempUntil,
		TempUnschedulableReason: BuildAccountSchedulingThresholdReason("quota threshold"),
	}
	repo := &quotaRecoveryRepositoryStub{}
	tester := &quotaRecoveryAccountTesterStub{result: &ScheduledTestResult{Status: "success"}}
	tempCache := &quotaRecoveryTempCacheStub{}
	blocker := &quotaRecoveryRuntimeBlockerStub{}
	svc := NewQuotaRecoveryService(repo, nil, tester, nil, tempCache, blocker, nil, nil)

	result := svc.refreshConnectivity(context.Background(), account)
	require.NoError(t, result.err)
	require.Equal(t, 1, result.probes)
	require.Equal(t, 1, result.cleared)
	require.True(t, tester.readOnly)
	_, _, _, mutations, _ := repo.snapshot()
	require.Len(t, mutations, 1)
	require.True(t, mutations[0].ClearGlobalRateLimit)
	require.True(t, mutations[0].ClearThresholdBlock)
	require.Equal(t, []int64{account.ID}, tempCache.deleted)
	require.Empty(t, tempCache.broadDeleted)
	require.Len(t, tempCache.expected, 1)
	require.Equal(t, tempUntil.Unix(), tempCache.expected[0].UntilUnix)
	require.Equal(t, "quota threshold", tempCache.expected[0].ErrorMessage)
	require.Equal(t, []int64{account.ID}, blocker.cleared)
	require.Empty(t, blocker.broadCleared)
}

func TestQuotaRecoveryKimiAPIAndMembershipRelayUseReadOnlyAPIKeyRecovery(t *testing.T) {
	tests := []struct {
		name          string
		baseURL       string
		upstreamModel string
	}{
		{name: "direct API", baseURL: "https://api.moonshot.ai/v1", upstreamModel: "kimi-k2.5"},
		{name: "membership relay", baseURL: "http://kimi-membership-relay:8090/v1", upstreamModel: "k3"},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account := &Account{
				ID:           int64(140 + i),
				Platform:     PlatformOpenAI,
				Type:         AccountTypeAPIKey,
				Status:       StatusError,
				Schedulable:  false,
				ErrorMessage: QuotaRecoveryPaymentErrorPrefix + " test balance exhausted",
				Concurrency:  1,
				GroupIDs:     []int64{91},
				Credentials: map[string]any{
					"api_key":  "test-key",
					"base_url": tt.baseURL,
					"model_mapping": map[string]any{
						"*": tt.upstreamModel,
					},
				},
				Extra: map[string]any{openai_compat.ExtraKeyResponsesSupported: false},
			}
			repo := &quotaRecoveryRepositoryStub{accounts: map[int64]*Account{account.ID: account}}
			upstreamBody := strings.Join([]string{
				`data: {"id":"chatcmpl_test","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"pong"},"finish_reason":null}]}`,
				"",
				`data: {"id":"chatcmpl_test","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
				"",
				"data: [DONE]",
				"",
			}, "\n")
			upstream := &httpUpstreamRecorder{resp: &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(upstreamBody)),
			}}
			accountTest := &AccountTestService{
				accountRepo:  repo,
				httpUpstream: upstream,
				cfg: &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{
					Enabled:           false,
					AllowInsecureHTTP: true,
				}}},
			}
			svc := NewQuotaRecoveryService(repo, nil, accountTest, nil, nil, nil, nil, nil)

			result := svc.refreshConnectivity(context.Background(), account)

			require.NoError(t, result.err)
			require.Equal(t, 1, result.probes)
			require.Equal(t, 1, result.cleared)
			require.NotNil(t, upstream.lastReq)
			require.Equal(t, strings.TrimSuffix(tt.baseURL, "/")+"/chat/completions", upstream.lastReq.URL.String())
			require.Equal(t, "Bearer test-key", upstream.lastReq.Header.Get("Authorization"))
			require.Equal(t, tt.upstreamModel, gjson.GetBytes(upstream.lastBody, "model").String())
			_, _, _, mutations, _ := repo.snapshot()
			require.Len(t, mutations, 1)
			require.True(t, mutations[0].ClearQuotaError)
			require.Equal(t, account.GroupIDs, mutations[0].Target.GroupIDs)
			require.Equal(t, account.Credentials, mutations[0].Target.Credentials)
		})
	}
}

func TestQuotaRecoveryBalanceErrorSuccessRestoresOnlyObservedQuotaError(t *testing.T) {
	t.Parallel()

	account := &Account{
		ID:           42,
		Platform:     PlatformOpenAI,
		Type:         AccountTypeAPIKey,
		Status:       StatusError,
		Schedulable:  false,
		ErrorMessage: QuotaRecoveryPaymentErrorPrefix + " insufficient balance",
		Credentials:  map[string]any{"api_key": "sk-test"},
	}
	repo := &quotaRecoveryRepositoryStub{}
	tester := &quotaRecoveryAccountTesterStub{result: &ScheduledTestResult{Status: "success"}}
	blocker := &quotaRecoveryRuntimeBlockerStub{
		snapshot: QuotaRecoveryRuntimeBlockSnapshot{
			Generation: 9,
			Until:      time.Now().Add(time.Hour),
			Reason:     "upstream_disable",
		},
		present: true,
	}
	svc := NewQuotaRecoveryService(repo, nil, tester, nil, nil, blocker, nil, nil)

	result := svc.refreshConnectivity(context.Background(), account)
	require.NoError(t, result.err)
	require.Equal(t, 1, result.probes)
	require.Equal(t, 1, result.cleared)
	require.True(t, tester.readOnly)
	_, _, _, mutations, _ := repo.snapshot()
	require.Len(t, mutations, 1)
	require.True(t, mutations[0].ClearQuotaError)
	require.False(t, mutations[0].Target.Schedulable)
	require.Equal(t, StatusError, mutations[0].Target.Status)
	require.Equal(t, []int64{account.ID}, blocker.cleared)
	require.Equal(t, []bool{true}, blocker.clearQuotaErrors)
}

func TestQuotaRecoveryBalanceErrorFailureNeverRestoresAccount(t *testing.T) {
	t.Parallel()

	account := &Account{
		ID:           43,
		Platform:     PlatformAnthropic,
		Type:         AccountTypeAPIKey,
		Status:       StatusError,
		Schedulable:  false,
		ErrorMessage: QuotaRecoveryCreditBalanceErrorPrefix + " balance is still empty",
		Credentials:  map[string]any{"api_key": "sk-ant"},
	}
	repo := &quotaRecoveryRepositoryStub{}
	tester := &quotaRecoveryAccountTesterStub{result: &ScheduledTestResult{Status: "failed", ErrorMessage: "still exhausted"}}
	svc := NewQuotaRecoveryService(repo, nil, tester, nil, nil, nil, nil, nil)

	result := svc.refreshConnectivity(context.Background(), account)
	require.ErrorContains(t, result.err, "still exhausted")
	require.Equal(t, 1, result.probes)
	require.Zero(t, result.cleared)
	_, _, _, mutations, _ := repo.snapshot()
	require.Empty(t, mutations)
}

func TestQuotaRecoveryProbeDoesNotClearConcurrentRuntimeBlock(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	resetAt := now.Add(time.Hour)
	account := &Account{
		ID: 43, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true,
		Credentials: map[string]any{"api_key": "sk-test"}, RateLimitedAt: &now, RateLimitResetAt: &resetAt,
	}
	repo := &quotaRecoveryRepositoryStub{}
	runtimeBlocker := &OpenAIGatewayService{}
	runtimeBlocker.BlockAccountScheduling(account, resetAt, "429")
	tester := &quotaRecoveryAccountTesterStub{
		result: &ScheduledTestResult{Status: "success"},
		hook: func() {
			runtimeBlocker.BlockAccountScheduling(account, resetAt.Add(time.Hour), "oauth_401")
		},
	}
	svc := NewQuotaRecoveryService(repo, nil, tester, nil, nil, runtimeBlocker, nil, nil)

	result := svc.refreshConnectivity(context.Background(), account)
	require.NoError(t, result.err)
	require.Equal(t, 1, result.cleared, "the unchanged database quota generation is still recovered")
	current, ok := runtimeBlocker.SnapshotQuotaRecoveryRuntimeBlock(account.ID)
	require.True(t, ok)
	require.Equal(t, "oauth_401", current.Reason)
	require.True(t, runtimeBlocker.isOpenAIAccountRuntimeBlocked(account))
}

type quotaRecoveryBroadOnlyRuntimeBlocker struct {
	cleared int
}

func (*quotaRecoveryBroadOnlyRuntimeBlocker) BlockAccountScheduling(*Account, time.Time, string) {}
func (b *quotaRecoveryBroadOnlyRuntimeBlocker) ClearAccountSchedulingBlock(int64) {
	b.cleared++
}

func TestQuotaRecoveryDoesNotFallbackToBroadRuntimeClear(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	resetAt := now.Add(time.Hour)
	account := &Account{
		ID: 44, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true,
		Credentials: map[string]any{"api_key": "sk-test"}, RateLimitedAt: &now, RateLimitResetAt: &resetAt,
	}
	repo := &quotaRecoveryRepositoryStub{}
	tester := &quotaRecoveryAccountTesterStub{result: &ScheduledTestResult{Status: "success"}}
	blocker := &quotaRecoveryBroadOnlyRuntimeBlocker{}
	svc := NewQuotaRecoveryService(repo, nil, tester, nil, nil, blocker, nil, nil)

	result := svc.refreshConnectivity(context.Background(), account)
	require.NoError(t, result.err)
	require.Equal(t, 1, result.cleared)
	require.Zero(t, blocker.cleared)
}

func TestQuotaRecoveryCASMissDoesNotRefreshRuntimeCaches(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	resetAt := now.Add(time.Hour)
	account := &Account{
		ID:               42,
		Platform:         PlatformAnthropic,
		Type:             AccountTypeAPIKey,
		Status:           StatusActive,
		Schedulable:      true,
		Credentials:      map[string]any{"api_key": "sk-test"},
		RateLimitedAt:    &now,
		RateLimitResetAt: &resetAt,
	}
	repo := &quotaRecoveryRepositoryStub{applyResultSet: true, applyResult: false}
	tester := &quotaRecoveryAccountTesterStub{result: &ScheduledTestResult{Status: "success"}}
	tempCache := &quotaRecoveryTempCacheStub{}
	blocker := &quotaRecoveryRuntimeBlockerStub{}
	svc := NewQuotaRecoveryService(repo, nil, tester, nil, tempCache, blocker, nil, nil)

	result := svc.refreshConnectivity(context.Background(), account)
	require.NoError(t, result.err)
	require.Zero(t, result.cleared)
	require.Empty(t, tempCache.deleted)
	require.Empty(t, blocker.cleared)
}

func TestQuotaRecoveryDoesNotProbeManualOrNonQuotaBlocks(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	resetAt := now.Add(time.Hour)
	tempUntil := now.Add(time.Hour)
	tests := []struct {
		name    string
		account *Account
	}{
		{
			name: "manual schedulable false",
			account: &Account{
				ID: 51, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive,
				Credentials:   map[string]any{"api_key": "sk-test"},
				RateLimitedAt: &now, RateLimitResetAt: &resetAt,
			},
		},
		{
			name: "non-threshold temporary block",
			account: &Account{
				ID: 52, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true,
				Credentials:            map[string]any{"api_key": "sk-test"},
				TempUnschedulableUntil: &tempUntil, TempUnschedulableReason: BuildTempUnschedReasonPayload("custom_rule", "operator policy"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &quotaRecoveryRepositoryStub{}
			tester := &quotaRecoveryAccountTesterStub{result: &ScheduledTestResult{Status: "success"}}
			svc := NewQuotaRecoveryService(repo, nil, tester, nil, nil, nil, nil, nil)

			result := svc.refreshConnectivity(context.Background(), tt.account)
			require.Zero(t, result.probes)
			require.Zero(t, tester.calls)
			_, _, _, mutations, _ := repo.snapshot()
			require.Empty(t, mutations)
		})
	}
}

func TestQuotaRecoveryAnthropicGlobalAndFableRecoveryAreIndependent(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	resetAt := now.Add(2 * time.Hour)
	tempUntil := now.Add(3 * time.Hour)
	fableLimit := map[string]any{
		"rate_limited_at":     now.Format(time.RFC3339),
		"rate_limit_reset_at": resetAt.Format(time.RFC3339),
		"reason":              anthropicFableWindowReason,
	}
	newResponse := func(globalUtilization float64) *ClaudeUsageResponse {
		response := &ClaudeUsageResponse{}
		response.FiveHour.Utilization = globalUtilization
		response.FiveHour.ResetsAt = resetAt.Format(time.RFC3339)
		response.SevenDay.Utilization = 20
		response.SevenDay.ResetsAt = resetAt.Add(5 * 24 * time.Hour).Format(time.RFC3339)
		response.SevenDayOverageIncluded.Utilization = 10
		response.SevenDayOverageIncluded.ResetsAt = resetAt.Add(5 * 24 * time.Hour).Format(time.RFC3339)
		return response
	}
	newAccount := func() *Account {
		return &Account{
			ID: 61, Platform: PlatformAnthropic, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true,
			Credentials:   map[string]any{"access_token": "oauth-token"},
			Extra:         map[string]any{modelRateLimitsKey: map[string]any{anthropicFableRateLimitKey: fableLimit}},
			RateLimitedAt: &now, RateLimitResetAt: &resetAt,
			TempUnschedulableUntil:  &tempUntil,
			TempUnschedulableReason: BuildAccountSchedulingThresholdReason("quota threshold"),
		}
	}

	t.Run("all authoritative windows available", func(t *testing.T) {
		repo := &quotaRecoveryRepositoryStub{}
		usage := &AccountUsageService{usageFetcher: &quotaRecoveryClaudeFetcherStub{response: newResponse(10)}}
		svc := NewQuotaRecoveryService(repo, usage, nil, nil, nil, nil, nil, nil)

		result := svc.refreshAnthropic(context.Background(), newAccount(), nil)
		require.NoError(t, result.err)
		require.Equal(t, 1, result.snapshots)
		require.Equal(t, 1, result.cleared)
		_, _, _, mutations, _ := repo.snapshot()
		require.Len(t, mutations, 1)
		mutation := mutations[0]
		require.True(t, mutation.ClearGlobalRateLimit)
		require.True(t, mutation.ClearThresholdBlock)
		require.Equal(t, []string{anthropicFableRateLimitKey}, mutation.ClearModelRateLimitKeys)
		require.Contains(t, mutation.ExtraUpdates, "passive_usage_sampled_at")
		require.NotNil(t, mutation.SessionWindowEnd)
	})

	t.Run("global exhausted but fable available", func(t *testing.T) {
		repo := &quotaRecoveryRepositoryStub{}
		usage := &AccountUsageService{usageFetcher: &quotaRecoveryClaudeFetcherStub{response: newResponse(100)}}
		svc := NewQuotaRecoveryService(repo, usage, nil, nil, nil, nil, nil, nil)

		result := svc.refreshAnthropic(context.Background(), newAccount(), nil)
		require.NoError(t, result.err)
		_, _, _, mutations, _ := repo.snapshot()
		require.Len(t, mutations, 1)
		mutation := mutations[0]
		require.False(t, mutation.ClearGlobalRateLimit)
		require.False(t, mutation.ClearThresholdBlock)
		require.Equal(t, []string{anthropicFableRateLimitKey}, mutation.ClearModelRateLimitKeys)
	})
}

func TestOpenAIQuotaRecoveryUsesTheTargetQuotaDimension(t *testing.T) {
	t.Parallel()

	global := &OpenAIRateLimit{Allowed: false, LimitReached: true, PrimaryWindow: &OpenAIRateLimitWindow{UsedPercent: 100}}
	spark := &OpenAIRateLimit{Allowed: true, PrimaryWindow: &OpenAIRateLimitWindow{UsedPercent: 15}}
	usage := &OpenAIQuotaUsage{
		RateLimit:            global,
		AdditionalRateLimits: []OpenAIAdditionalRateLimit{{MeteredFeature: "CODEX_BENGALFOX", RateLimit: spark}},
	}

	normalAccount := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	require.Same(t, global, openAIQuotaLimitForRecovery(normalAccount, usage))
	require.False(t, authoritativeOpenAIAvailable(openAIQuotaLimitForRecovery(normalAccount, usage)))

	parentID := int64(70)
	sparkAccount := &Account{
		Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		ParentAccountID: &parentID, QuotaDimension: QuotaDimensionSpark,
	}
	require.Same(t, spark, openAIQuotaLimitForRecovery(sparkAccount, usage))
	require.True(t, authoritativeOpenAIAvailable(openAIQuotaLimitForRecovery(sparkAccount, usage)))

	usage.AdditionalRateLimits = nil
	require.Nil(t, openAIQuotaLimitForRecovery(sparkAccount, usage))
	require.False(t, authoritativeOpenAIAvailable(nil))
}

func TestGrokQuotaRecoveryRequiresSuccessfulActiveProbe(t *testing.T) {
	t.Parallel()

	require.True(t, grokActiveProbeAvailable(&GrokQuotaProbeResult{StatusCode: http.StatusOK}))
	require.False(t, grokActiveProbeAvailable(&GrokQuotaProbeResult{StatusCode: http.StatusTooManyRequests}))
	require.False(t, grokActiveProbeAvailable(&GrokQuotaProbeResult{StatusCode: http.StatusUnauthorized}))
}

func TestGrokQuotaRecoveryUsesFreshActiveSnapshotForThresholdRecovery(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	until := now.Add(time.Hour)
	account := &Account{
		ID: 72, Platform: PlatformGrok, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true,
		Credentials: map[string]any{
			"access_token": "access-token", "refresh_token": "refresh-token",
			"expires_at": now.Add(time.Hour).Format(time.RFC3339),
		},
		Extra: map[string]any{
			"grok_sched_utilization": 95.0,
			"grok_sched_reset_at":    until.Format(time.RFC3339),
		},
		TempUnschedulableUntil:  &until,
		TempUnschedulableReason: BuildAccountSchedulingThresholdReason("grok quota threshold"),
	}
	repo := &quotaRecoveryRepositoryStub{accounts: map[int64]*Account{account.ID: account}}
	upstream := &grokHybridUpstream{}
	grokQuota := NewGrokQuotaService(repo, nil, NewGrokTokenProvider(repo, nil), upstream, nil)
	usage := &AccountUsageService{grokQuotaService: grokQuota}
	blocker := &quotaRecoveryRuntimeBlockerStub{}
	svc := NewQuotaRecoveryService(repo, usage, nil, nil, nil, blocker, nil, nil)

	result := svc.refreshGrok(context.Background(), account, map[string]int{PlatformGrok: 80})
	require.NoError(t, result.err)
	require.Equal(t, 2, result.probes)
	require.Equal(t, 2, result.snapshots)
	require.Equal(t, 1, result.cleared)
	_, _, _, mutations, _ := repo.snapshot()
	require.Len(t, mutations, 2)
	activeMutation := mutations[1]
	require.True(t, activeMutation.ClearThresholdBlock)
	require.Contains(t, activeMutation.ExtraUpdates, grokQuotaSnapshotExtraKey)
	require.InDelta(t, 25.0, activeMutation.ExtraUpdates["grok_sched_utilization"], 0.001)
	require.Equal(t, []int64{account.ID}, blocker.cleared)
}

func TestAntigravityQuotaRecoveryClearsOnlyAuthoritativelyAvailableModelKeys(t *testing.T) {
	t.Parallel()

	limit := func(reason string) map[string]any {
		payload := map[string]any{"rate_limit_reset_at": "2099-01-01T00:00:00Z"}
		if reason != "" {
			payload["reason"] = reason
		}
		return payload
	}
	account := &Account{
		ID: 81, Platform: PlatformAntigravity, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true,
		Extra: map[string]any{modelRateLimitsKey: map[string]any{
			antigravityGeminiModelRateLimitKey: limit(""),
			creditsExhaustedKey:                limit(""),
			"claude-sonnet-4-5":                limit(""),
			"protected-model":                  limit("operator_policy"),
		}},
	}
	usage := &UsageInfo{
		AntigravityQuota: map[string]*AntigravityModelQuota{
			"gemini-2.5-pro":    {Utilization: 20},
			"claude-sonnet-4-5": {Utilization: 30},
			"protected-model":   {Utilization: 10},
		},
		AICredits: []AICredit{{Amount: 2, MinimumBalance: 1}},
	}
	mutation := newQuotaRecoveryMutation(account, account, nil, nil)
	addRecoverableAntigravityModelLimits(&mutation, account, usage)

	want := []string{creditsExhaustedKey, antigravityGeminiModelRateLimitKey, "claude-sonnet-4-5"}
	sort.Strings(want)
	require.Equal(t, want, mutation.ClearModelRateLimitKeys)
	require.NotContains(t, mutation.ExpectedModelRateLimits, "protected-model")
	require.True(t, authoritativeAntigravityAvailable(account, usage))

	usage.AntigravityQuota["gemini-2.5-pro"].Utilization = 100
	require.True(t, authoritativeAntigravityAvailable(account, usage), "one available model makes the account callable")
	usage.AntigravityQuota["claude-sonnet-4-5"].Utilization = 100
	usage.AntigravityQuota["protected-model"].Utilization = 100
	require.False(t, authoritativeAntigravityAvailable(account, usage), "credits are not a call path until overages is enabled")

	account.Extra["allow_overages"] = true
	require.True(t, authoritativeAntigravityAvailable(account, usage), "enabled additional credits make the account callable")
	usage.AICredits[0].Amount = usage.AICredits[0].MinimumBalance
	require.False(t, authoritativeAntigravityAvailable(account, usage))
}

func TestQuotaRecoveryMutationHelpersPreserveManualAndNonThresholdBlocks(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	resetAt := now.Add(time.Hour)
	tempUntil := now.Add(2 * time.Hour)
	account := &Account{
		ID: 91, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive,
		RateLimitedAt: &now, RateLimitResetAt: &resetAt,
		TempUnschedulableUntil:  &tempUntil,
		TempUnschedulableReason: BuildTempUnschedReasonPayload("custom_rule", "do not clear"),
		Extra:                   map[string]any{modelRateLimitsKey: map[string]any{"model": map[string]any{"rate_limit_reset_at": resetAt.Format(time.RFC3339)}}},
	}
	mutation := newQuotaRecoveryMutation(account, account, nil, nil)
	addObservedAccountBlocks(&mutation, account, account, nil, true)
	addExpectedModelLimit(&mutation, account, "model", "")
	require.False(t, mutation.ClearGlobalRateLimit)
	require.False(t, mutation.ClearThresholdBlock)
	require.Empty(t, mutation.ClearModelRateLimitKeys)

	account.Schedulable = true
	mutation = newQuotaRecoveryMutation(account, account, nil, nil)
	addObservedAccountBlocks(&mutation, account, account, nil, true)
	require.True(t, mutation.ClearGlobalRateLimit)
	require.False(t, mutation.ClearThresholdBlock)
}

func TestQuotaRecoveryIdentityAllowsRestoreOnlyHealthyParentOrOwnedBalanceError(t *testing.T) {
	t.Parallel()

	target := &Account{ID: 201, Status: StatusError, ErrorMessage: QuotaRecoveryPaymentErrorPrefix + " exhausted"}
	tests := []struct {
		name     string
		identity *Account
		want     bool
	}{
		{name: "healthy parent", identity: &Account{ID: 202, Status: StatusActive, Schedulable: true}, want: true},
		{name: "manually paused parent", identity: &Account{ID: 202, Status: StatusActive}},
		{name: "disabled parent", identity: &Account{ID: 202, Status: StatusDisabled, Schedulable: true}},
		{name: "different balance-error parent", identity: &Account{ID: 202, Status: StatusError, ErrorMessage: QuotaRecoveryPaymentErrorPrefix + " exhausted"}},
		{name: "target owns balance error", identity: target, want: true},
		{name: "target owns authentication error", identity: &Account{ID: target.ID, Status: StatusError, ErrorMessage: "Authentication failed (401)"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, quotaRecoveryIdentityAllowsRestore(target, tt.identity))
		})
	}
}

func TestAntigravityPersistedQuotaRecoverySnapshotCanRefreshBalanceDisplay(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	account := &Account{Extra: map[string]any{
		QuotaRecoveryAntigravityUsageSnapshotExtraKey: map[string]any{
			"updated_at": now.Format(time.RFC3339),
			"antigravity_quota": map[string]any{
				"gemini-2.5-pro": map[string]any{"utilization": 25, "reset_time": now.Add(time.Hour).Format(time.RFC3339)},
			},
			"ai_credits": []any{map[string]any{"credit_type": "GOOGLE_ONE_AI", "amount": 3.5, "minimum_balance": 1.0}},
		},
	}}

	usage := antigravityUsageSnapshotFromExtra(account)
	require.NotNil(t, usage)
	require.WithinDuration(t, now, *usage.UpdatedAt, time.Second)
	require.Equal(t, 25, usage.AntigravityQuota["gemini-2.5-pro"].Utilization)
	require.Equal(t, 3.5, usage.AICredits[0].Amount)
}

func TestAntigravityPersistedQuotaRecoverySnapshotUsesNormalCacheTTL(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 14, 8, 0, 0, 0, time.UTC)
	freshAt := now.Add(-2 * time.Minute)
	staleAt := now.Add(-4 * time.Minute)
	futureAt := now.Add(2 * time.Minute)
	fresh := &UsageInfo{UpdatedAt: &freshAt, AntigravityQuota: map[string]*AntigravityModelQuota{
		"model": {Utilization: 25},
	}}
	stale := &UsageInfo{UpdatedAt: &staleAt, AntigravityQuota: map[string]*AntigravityModelQuota{
		"model": {Utilization: 25},
	}}
	future := &UsageInfo{UpdatedAt: &futureAt, AntigravityQuota: map[string]*AntigravityModelQuota{
		"model": {Utilization: 25},
	}}

	require.True(t, shouldUsePersistedAntigravityUsageSnapshot(fresh, now))
	require.False(t, shouldUsePersistedAntigravityUsageSnapshot(stale, now), "persisted state must not suppress normal live refresh for four hours")
	require.False(t, shouldUsePersistedAntigravityUsageSnapshot(future, now))
}

func TestQuotaRecoveryProbeEligibilityCoversSupportedCredentialMatrix(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	resetAt := now.Add(time.Hour)
	validServiceAccount := `{"project_id":"vertex-project","private_key":"private-key","client_email":"svc@example.com"}`
	blocked := func(platform, accountType string, credentials map[string]any) *Account {
		return &Account{
			ID: 101, Platform: platform, Type: accountType, Status: StatusActive, Schedulable: true,
			Credentials: credentials, RateLimitedAt: &now, RateLimitResetAt: &resetAt,
		}
	}
	tests := []struct {
		name          string
		account       *Account
		authoritative bool
		connectivity  bool
	}{
		{
			name: "anthropic oauth has authoritative quota",
			account: &Account{ID: 1, Platform: PlatformAnthropic, Type: AccountTypeOAuth, Status: StatusActive,
				Credentials: map[string]any{"access_token": "oauth-token"}},
			authoritative: true,
		},
		{name: "anthropic setup token uses connectivity", account: blocked(PlatformAnthropic, AccountTypeSetupToken, map[string]any{"access_token": "setup-token"}), connectivity: true},
		{name: "anthropic api key uses connectivity", account: blocked(PlatformAnthropic, AccountTypeAPIKey, map[string]any{"api_key": "sk-ant"}), connectivity: true},
		{name: "anthropic bedrock sigv4 uses connectivity", account: blocked(PlatformAnthropic, AccountTypeBedrock, map[string]any{"auth_mode": "sigv4", "aws_access_key_id": "AKIA", "aws_secret_access_key": "secret"}), connectivity: true},
		{name: "anthropic bedrock api key uses connectivity", account: blocked(PlatformAnthropic, AccountTypeBedrock, map[string]any{"auth_mode": "apikey", "api_key": "bedrock-key"}), connectivity: true},
		{name: "anthropic vertex uses connectivity", account: blocked(PlatformAnthropic, AccountTypeServiceAccount, map[string]any{"service_account_json": validServiceAccount}), connectivity: true},
		{name: "openai api key uses connectivity", account: blocked(PlatformOpenAI, AccountTypeAPIKey, map[string]any{"api_key": "sk-openai"}), connectivity: true},
		{name: "gemini oauth uses connectivity", account: blocked(PlatformGemini, AccountTypeOAuth, map[string]any{"refresh_token": "refresh-token"}), connectivity: true},
		{name: "gemini api key uses connectivity", account: blocked(PlatformGemini, AccountTypeAPIKey, map[string]any{"api_key": "gemini-key"}), connectivity: true},
		{name: "gemini vertex uses connectivity", account: blocked(PlatformGemini, AccountTypeServiceAccount, map[string]any{"service_account_json": validServiceAccount}), connectivity: true},
		{name: "antigravity upstream uses connectivity", account: blocked(PlatformAntigravity, AccountTypeUpstream, map[string]any{"api_key": "relay-key", "base_url": "https://relay.example.com"}), connectivity: true},
		{name: "grok api key uses connectivity", account: blocked(PlatformGrok, AccountTypeAPIKey, map[string]any{"api_key": "xai-key"}), connectivity: true},
		{name: "missing upstream URL is skipped", account: blocked(PlatformAntigravity, AccountTypeUpstream, map[string]any{"api_key": "relay-key"})},
		{name: "invalid service account is skipped", account: blocked(PlatformGemini, AccountTypeServiceAccount, map[string]any{"service_account_json": `{}`})},
		{name: "unsupported platform/type pair is skipped", account: blocked(PlatformOpenAI, AccountTypeSetupToken, map[string]any{"access_token": "token"})},
		{name: "unblocked Gemini account is skipped", account: &Account{ID: 2, Platform: PlatformGemini, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Credentials: map[string]any{"api_key": "key"}}},
		{name: "manual schedulable false is never connectivity-probed", account: &Account{ID: 3, Platform: PlatformGemini, Type: AccountTypeAPIKey, Status: StatusActive, Credentials: map[string]any{"api_key": "key"}, RateLimitedAt: &now, RateLimitResetAt: &resetAt}},
		{name: "manual schedulable false still refreshes authoritative balance", account: &Account{ID: 4, Platform: PlatformAntigravity, Type: AccountTypeOAuth, Status: StatusActive, Credentials: map[string]any{"access_token": "token"}}, authoritative: true},
		{
			name: "balance error oauth still refreshes authoritative balance",
			account: &Account{ID: 5, Platform: PlatformAnthropic, Type: AccountTypeOAuth, Status: StatusError,
				ErrorMessage: QuotaRecoveryCreditBalanceErrorPrefix + " balance exhausted", Credentials: map[string]any{"access_token": "token"}},
			authoritative: true,
		},
		{
			name: "balance error api key uses connectivity",
			account: &Account{ID: 6, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusError,
				ErrorMessage: QuotaRecoveryPaymentErrorPrefix + " billing issue", Credentials: map[string]any{"api_key": "key"}},
			connectivity: true,
		},
		{
			name: "authentication error is never probed",
			account: &Account{ID: 7, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusError,
				ErrorMessage: "Authentication failed (401): invalid API key", Credentials: map[string]any{"api_key": "key"}},
		},
		{
			name: "schedulable balance error is not recovery-owned",
			account: &Account{ID: 8, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusError, Schedulable: true,
				ErrorMessage: QuotaRecoveryPaymentErrorPrefix + " billing issue", Credentials: map[string]any{"api_key": "key"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.authoritative, quotaRecoveryAuthoritativeProbeEligible(tt.account))
			require.Equal(t, tt.connectivity, quotaRecoveryConnectivityProbeEligible(tt.account))
			require.Equal(t, tt.authoritative || tt.connectivity, quotaRecoveryProbeEligible(tt.account))
		})
	}
}

func TestQuotaRecoveryAnthropicSetupTokenDispatchesToReadOnlyConnectivityProbe(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	resetAt := now.Add(time.Hour)
	account := &Account{
		ID: 111, Platform: PlatformAnthropic, Type: AccountTypeSetupToken, Status: StatusActive, Schedulable: true,
		Credentials: map[string]any{"access_token": "setup-token"}, RateLimitedAt: &now, RateLimitResetAt: &resetAt,
	}
	repo := &quotaRecoveryRepositoryStub{}
	tester := &quotaRecoveryAccountTesterStub{result: &ScheduledTestResult{Status: "success"}}
	svc := NewQuotaRecoveryService(repo, nil, tester, nil, nil, nil, nil, nil)

	result := svc.processAccount(context.Background(), account, nil)
	require.NoError(t, result.err)
	require.Equal(t, 1, result.probes)
	require.Equal(t, 1, result.cleared)
	require.Equal(t, account.ID, tester.account)
	require.True(t, tester.readOnly)
	_, _, _, mutations, _ := repo.snapshot()
	require.Len(t, mutations, 1)
	require.True(t, mutations[0].ClearGlobalRateLimit)
}

func TestQuotaRecoveryConnectivityFailureNeverClearsObservedState(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)
	resetAt := now.Add(time.Hour)
	account := &Account{
		ID: 121, Platform: PlatformGemini, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true,
		Credentials: map[string]any{"access_token": "oauth-token"}, RateLimitedAt: &now, RateLimitResetAt: &resetAt,
	}
	repo := &quotaRecoveryRepositoryStub{}
	tester := &quotaRecoveryAccountTesterStub{result: &ScheduledTestResult{Status: "failed", ErrorMessage: "quota still exhausted"}}
	svc := NewQuotaRecoveryService(repo, nil, tester, nil, nil, nil, nil, nil)

	result := svc.refreshConnectivity(context.Background(), account)
	require.ErrorContains(t, result.err, "quota still exhausted")
	require.Equal(t, 1, result.probes)
	require.Zero(t, result.cleared)
	require.True(t, tester.readOnly)
	_, _, _, mutations, _ := repo.snapshot()
	require.Empty(t, mutations)
}
