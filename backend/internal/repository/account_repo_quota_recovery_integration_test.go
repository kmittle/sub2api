//go:build integration

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestQuotaRecoverySlotClaimAndReleaseUsesExactMonotonicSlot(t *testing.T) {
	ctx := context.Background()
	tx := testEntTx(t)
	repo := newAccountRepositoryWithSQL(tx.Client(), tx, nil)
	_, err := tx.ExecContext(ctx, "DELETE FROM settings WHERE key = $1", service.QuotaRecoverySlotSettingKey)
	require.NoError(t, err)

	slot := time.Date(2026, 8, 14, 4, 0, 0, 0, time.UTC)
	claimed, err := repo.ClaimQuotaRecoverySlot(ctx, slot)
	require.NoError(t, err)
	require.True(t, claimed)

	claimed, err = repo.ClaimQuotaRecoverySlot(ctx, slot)
	require.NoError(t, err)
	require.False(t, claimed, "the same slot must be deduplicated")

	claimed, err = repo.ClaimQuotaRecoverySlot(ctx, slot.Add(-4*time.Hour))
	require.NoError(t, err)
	require.False(t, claimed, "an older runner must not move the marker backwards")

	newer := slot.Add(4 * time.Hour)
	claimed, err = repo.ClaimQuotaRecoverySlot(ctx, newer)
	require.NoError(t, err)
	require.True(t, claimed)

	released, err := repo.ReleaseQuotaRecoverySlot(ctx, slot)
	require.NoError(t, err)
	require.False(t, released, "a failed old cycle must not delete a newer claim")

	released, err = repo.ReleaseQuotaRecoverySlot(ctx, newer)
	require.NoError(t, err)
	require.True(t, released)
}

func TestApplyQuotaRecoveryMutationAtomicallyClearsObservedBlocksAndPublishesSnapshot(t *testing.T) {
	ctx := context.Background()
	tx := testEntTx(t)
	cache := &schedulerCacheRecorder{}
	repo := newAccountRepositoryWithSQL(tx.Client(), tx, cache)

	now := time.Date(2026, 8, 14, 4, 5, 0, 0, time.UTC)
	globalReset := now.Add(2 * time.Hour)
	tempUntil := now.Add(3 * time.Hour)
	sessionEnd := now.Add(5 * time.Hour)
	tempReason := service.BuildAccountSchedulingThresholdReason("quota threshold")
	account := mustCreateAccount(t, tx.Client(), &service.Account{
		Name: "quota-recovery-atomic", Platform: service.PlatformAnthropic, Type: service.AccountTypeOAuth,
		Status: service.StatusActive, Schedulable: true,
		Credentials: map[string]any{"access_token": "oauth-token", "account_id": "upstream-1"},
		Extra: map[string]any{
			"quota_used": 123.5,
			"model_rate_limits": map[string]any{
				"model-a": map[string]any{
					"rate_limited_at": now.Format(time.RFC3339), "rate_limit_reset_at": globalReset.Format(time.RFC3339),
				},
				"AICredits": map[string]any{
					"rate_limited_at": now.Format(time.RFC3339), "rate_limit_reset_at": globalReset.Format(time.RFC3339),
				},
				"operator-protected": map[string]any{
					"rate_limit_reset_at": globalReset.Format(time.RFC3339), "reason": "operator_policy",
				},
			},
		},
		RateLimitedAt: &now, RateLimitResetAt: &globalReset,
	})
	_, err := tx.ExecContext(ctx, `
		UPDATE accounts
		SET temp_unschedulable_until = $2, temp_unschedulable_reason = $3
		WHERE id = $1`, account.ID, tempUntil, tempReason)
	require.NoError(t, err)

	observed, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)
	limits := observed.Extra["model_rate_limits"].(map[string]any)
	mutation := service.QuotaRecoveryMutation{
		Target: observed, Identity: observed,
		ExtraUpdates: map[string]any{
			"quota_recovery_test_snapshot": map[string]any{"remaining": 7.25},
		},
		SessionWindowEnd:         &sessionEnd,
		ClearGlobalRateLimit:     true,
		ExpectedRateLimitedAt:    observed.RateLimitedAt,
		ExpectedRateLimitResetAt: observed.RateLimitResetAt,
		ClearThresholdBlock:      true,
		ExpectedTempUntil:        observed.TempUnschedulableUntil,
		ExpectedTempReason:       observed.TempUnschedulableReason,
		ClearModelRateLimitKeys:  []string{"model-a", "AICredits"},
		ExpectedModelRateLimits: map[string]any{
			"model-a":   limits["model-a"],
			"AICredits": limits["AICredits"],
		},
	}

	applied, err := repo.ApplyQuotaRecoveryMutation(ctx, mutation)
	require.NoError(t, err)
	require.True(t, applied)

	got, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)
	require.True(t, got.Schedulable, "manual schedulable state must not be rewritten")
	require.Nil(t, got.RateLimitedAt)
	require.Nil(t, got.RateLimitResetAt)
	require.Nil(t, got.TempUnschedulableUntil)
	require.Empty(t, got.TempUnschedulableReason)
	require.WithinDuration(t, sessionEnd, *got.SessionWindowEnd, time.Microsecond)
	require.Equal(t, 123.5, got.Extra["quota_used"], "local quota counters must be preserved")
	require.Equal(t, 7.25, got.Extra["quota_recovery_test_snapshot"].(map[string]any)["remaining"])
	gotLimits := got.Extra["model_rate_limits"].(map[string]any)
	require.NotContains(t, gotLimits, "model-a")
	require.NotContains(t, gotLimits, "AICredits")
	require.Contains(t, gotLimits, "operator-protected")

	var outboxCount int
	require.NoError(t, scanSingleRow(ctx, tx,
		"SELECT COUNT(*) FROM scheduler_outbox WHERE event_type = $1 AND account_id = $2",
		[]any{service.SchedulerOutboxEventAccountChanged, account.ID}, &outboxCount))
	require.Equal(t, 1, outboxCount)
	require.Len(t, cache.setAccounts, 1)
	require.NoError(t, cache.setCtxErr)
	require.Equal(t, account.ID, cache.setAccounts[0].ID)
	require.Nil(t, cache.setAccounts[0].RateLimitedAt)
	require.Nil(t, cache.setAccounts[0].TempUnschedulableUntil)
}

func TestApplyQuotaRecoveryMutationCASMissLeavesNewGenerationUntouched(t *testing.T) {
	ctx := context.Background()
	tx := testEntTx(t)
	cache := &schedulerCacheRecorder{}
	repo := newAccountRepositoryWithSQL(tx.Client(), tx, cache)

	now := time.Date(2026, 8, 14, 8, 5, 0, 0, time.UTC)
	resetAt := now.Add(time.Hour)
	account := mustCreateAccount(t, tx.Client(), &service.Account{
		Name: "quota-recovery-cas-miss", Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
		Status: service.StatusActive, Schedulable: true,
		Credentials:   map[string]any{"access_token": "old-token", "chatgpt_account_id": "account-1"},
		RateLimitedAt: &now, RateLimitResetAt: &resetAt,
	})
	observed, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, `
		UPDATE accounts
		SET credentials = jsonb_set(credentials, '{access_token}', '"new-token"'::jsonb), updated_at = NOW()
		WHERE id = $1`, account.ID)
	require.NoError(t, err)

	applied, err := repo.ApplyQuotaRecoveryMutation(ctx, service.QuotaRecoveryMutation{
		Target: observed, Identity: observed,
		ExtraUpdates:             map[string]any{"quota_recovery_test_snapshot": map[string]any{"remaining": 9}},
		ClearGlobalRateLimit:     true,
		ExpectedRateLimitedAt:    observed.RateLimitedAt,
		ExpectedRateLimitResetAt: observed.RateLimitResetAt,
	})
	require.NoError(t, err)
	require.False(t, applied)

	got, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)
	require.Equal(t, "new-token", got.GetCredential("access_token"))
	require.NotNil(t, got.RateLimitedAt)
	require.NotNil(t, got.RateLimitResetAt)
	require.NotContains(t, got.Extra, "quota_recovery_test_snapshot")

	var outboxCount int
	require.NoError(t, scanSingleRow(ctx, tx,
		"SELECT COUNT(*) FROM scheduler_outbox WHERE event_type = $1 AND account_id = $2",
		[]any{service.SchedulerOutboxEventAccountChanged, account.ID}, &outboxCount))
	require.Zero(t, outboxCount)
	require.Empty(t, cache.setAccounts)
}

func TestApplyQuotaRecoveryMutationCASMissesConcurrentAvailabilityPolicyChange(t *testing.T) {
	ctx := context.Background()
	tx := testEntTx(t)
	repo := newAccountRepositoryWithSQL(tx.Client(), tx, nil)

	account := mustCreateAccount(t, tx.Client(), &service.Account{
		Name: "quota-recovery-policy-cas", Platform: service.PlatformAntigravity, Type: service.AccountTypeOAuth,
		Status: service.StatusActive, Schedulable: true,
		Credentials: map[string]any{"access_token": "oauth-token", "project_id": "project-1"},
		Extra:       map[string]any{"allow_overages": false},
	})
	observed, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, `
		UPDATE accounts
		SET extra = jsonb_set(extra, '{allow_overages}', 'true'::jsonb), updated_at = NOW()
		WHERE id = $1`, account.ID)
	require.NoError(t, err)

	applied, err := repo.ApplyQuotaRecoveryMutation(ctx, service.QuotaRecoveryMutation{
		Target: observed, Identity: observed,
		ExtraUpdates: map[string]any{"antigravity_usage_snapshot": map[string]any{"updated_at": time.Now().UTC()}},
	})
	require.NoError(t, err)
	require.False(t, applied)

	got, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)
	require.Equal(t, true, got.Extra["allow_overages"])
	require.NotContains(t, got.Extra, "antigravity_usage_snapshot")
}

func TestApplyQuotaRecoveryMutationRestoresOnlyExactBalanceErrorGeneration(t *testing.T) {
	ctx := context.Background()
	tx := testEntTx(t)
	cache := &schedulerCacheRecorder{}
	repo := newAccountRepositoryWithSQL(tx.Client(), tx, cache)
	group := mustCreateGroup(t, tx.Client(), &service.Group{
		Name:     "quota-recovery-original-group",
		Platform: service.PlatformOpenAI,
	})

	createBalanceError := func(name, errorMessage string) *service.Account {
		account := mustCreateAccount(t, tx.Client(), &service.Account{
			Name: name, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
			Status: service.StatusError, ErrorMessage: errorMessage,
			Credentials: map[string]any{
				"api_key":  "sk-test",
				"base_url": "http://kimi-membership-relay:8090/v1",
				"model_mapping": map[string]any{
					"gpt-5.6-sol": "k3",
				},
			},
			Extra: map[string]any{"operator_setting": "keep"},
		})
		require.NoError(t, repo.SetSchedulable(ctx, account.ID, false))
		observed, err := repo.GetByID(ctx, account.ID)
		require.NoError(t, err)
		return observed
	}

	recoverable := createBalanceError(
		"quota-recovery-balance-error",
		service.QuotaRecoveryPaymentErrorPrefix+" insufficient balance",
	)
	mustBindAccountToGroup(t, tx.Client(), recoverable.ID, group.ID, 23)
	recoverable, err := repo.GetByID(ctx, recoverable.ID)
	require.NoError(t, err)
	require.Equal(t, []int64{group.ID}, recoverable.GroupIDs)
	originalCredentials := recoverable.Credentials
	cacheEventsBeforeRecovery := len(cache.setAccounts)

	applied, err := repo.ApplyQuotaRecoveryMutation(ctx, service.QuotaRecoveryMutation{
		Target: recoverable, Identity: recoverable, ClearQuotaError: true,
	})
	require.NoError(t, err)
	require.True(t, applied)

	got, err := repo.GetByID(ctx, recoverable.ID)
	require.NoError(t, err)
	require.Equal(t, service.StatusActive, got.Status)
	require.False(t, got.Schedulable, "legacy recovery must preserve the administrator-owned scheduling switch")
	require.Empty(t, got.ErrorMessage)
	require.Equal(t, "keep", got.Extra["operator_setting"], "an empty model-limit key set must preserve existing extra")
	require.Equal(t, originalCredentials, got.Credentials, "quota recovery must not rewrite model mapping or upstream identity")
	require.Equal(t, []int64{group.ID}, got.GroupIDs, "quota recovery must preserve the original scheduling groups")
	var groupPriority int
	require.NoError(t, scanSingleRow(ctx, tx,
		"SELECT priority FROM account_groups WHERE account_id = $1 AND group_id = $2",
		[]any{recoverable.ID, group.ID}, &groupPriority))
	require.Equal(t, 23, groupPriority)
	require.Len(t, cache.setAccounts, cacheEventsBeforeRecovery+1)
	refreshed := cache.setAccounts[len(cache.setAccounts)-1]
	require.Equal(t, recoverable.ID, refreshed.ID)
	require.Equal(t, []int64{group.ID}, refreshed.GroupIDs, "the immediate scheduler refresh must carry the original groups")
	require.Equal(t, originalCredentials, refreshed.Credentials)

	stale := createBalanceError(
		"quota-recovery-balance-error-cas",
		service.QuotaRecoveryCreditBalanceErrorPrefix+" first generation",
	)
	newError := service.QuotaRecoveryCreditBalanceErrorPrefix + " newer generation"
	_, err = tx.ExecContext(ctx, "UPDATE accounts SET error_message = $2, updated_at = NOW() WHERE id = $1", stale.ID, newError)
	require.NoError(t, err)
	cacheEventsBeforeCASMiss := len(cache.setAccounts)

	applied, err = repo.ApplyQuotaRecoveryMutation(ctx, service.QuotaRecoveryMutation{
		Target: stale, Identity: stale, ClearQuotaError: true,
	})
	require.NoError(t, err)
	require.False(t, applied)

	got, err = repo.GetByID(ctx, stale.ID)
	require.NoError(t, err)
	require.Equal(t, service.StatusError, got.Status)
	require.False(t, got.Schedulable)
	require.Equal(t, newError, got.ErrorMessage)
	require.Len(t, cache.setAccounts, cacheEventsBeforeCASMiss, "a CAS miss must not refresh the scheduler snapshot")
}

func TestApplyQuotaRecoveryMutationClearsQuotaExhaustionWithoutLosingConcurrentManualPause(t *testing.T) {
	ctx := context.Background()
	tx := testEntTx(t)
	repo := newAccountRepositoryWithSQL(tx.Client(), tx, nil)
	limitedAt := time.Now().UTC().Truncate(time.Microsecond)
	account := mustCreateAccount(t, tx.Client(), &service.Account{
		Name: "quota-recovery-manual-pause", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
		Status: service.StatusActive, Schedulable: true,
		Credentials:   map[string]any{"api_key": "test-key", "model_mapping": map[string]any{"gpt-test": "upstream-test"}},
		Extra:         map[string]any{"operator_setting": "keep"},
		RateLimitedAt: &limitedAt,
	})
	observed, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)

	require.NoError(t, repo.SetSchedulable(ctx, account.ID, false))

	applied, err := repo.ApplyQuotaRecoveryMutation(ctx, service.QuotaRecoveryMutation{
		Target: observed, Identity: observed,
		ClearQuotaExhaustion:     true,
		ExpectedQuotaExhaustedAt: observed.RateLimitedAt,
		ExtraUpdates:             map[string]any{"quota_recovery_snapshot": map[string]any{"available": true}},
	})
	require.NoError(t, err)
	require.True(t, applied)

	got, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)
	require.Equal(t, service.StatusActive, got.Status)
	require.False(t, got.Schedulable, "a manual pause during the probe must survive the CAS update")
	require.Nil(t, got.RateLimitedAt)
	require.Nil(t, got.RateLimitResetAt)
	require.Equal(t, "keep", got.Extra["operator_setting"])
	require.Equal(t, observed.Credentials, got.Credentials, "model mapping and credential identity must remain byte-equivalent")
}

func TestApplyQuotaRecoveryMutationClearsTimedRateLimitWithoutLosingConcurrentManualPause(t *testing.T) {
	ctx := context.Background()
	tx := testEntTx(t)
	repo := newAccountRepositoryWithSQL(tx.Client(), tx, nil)
	limitedAt := time.Now().UTC().Truncate(time.Microsecond)
	resetAt := limitedAt.Add(time.Hour)
	account := mustCreateAccount(t, tx.Client(), &service.Account{
		Name: "quota-recovery-timed-manual-pause", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
		Status: service.StatusActive, Schedulable: true,
		Credentials:      map[string]any{"api_key": "test-key", "model_mapping": map[string]any{"gpt-test": "upstream-test"}},
		Extra:            map[string]any{"operator_setting": "keep"},
		RateLimitedAt:    &limitedAt,
		RateLimitResetAt: &resetAt,
	})
	observed, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)

	require.NoError(t, repo.SetSchedulable(ctx, account.ID, false))

	applied, err := repo.ApplyQuotaRecoveryMutation(ctx, service.QuotaRecoveryMutation{
		Target: observed, Identity: observed,
		ClearGlobalRateLimit:     true,
		ExpectedRateLimitedAt:    observed.RateLimitedAt,
		ExpectedRateLimitResetAt: observed.RateLimitResetAt,
		ExtraUpdates:             map[string]any{"quota_recovery_snapshot": map[string]any{"available": true}},
	})
	require.NoError(t, err)
	require.True(t, applied)

	got, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)
	require.Equal(t, service.StatusActive, got.Status)
	require.False(t, got.Schedulable, "a manual pause during the probe must survive the CAS update")
	require.Nil(t, got.RateLimitedAt)
	require.Nil(t, got.RateLimitResetAt)
	require.Equal(t, "keep", got.Extra["operator_setting"])
	require.Equal(t, observed.Credentials, got.Credentials, "model mapping and credential identity must remain byte-equivalent")
}

func TestApplyQuotaRecoveryMutationClearsQuotaExhaustionWithoutLosingConcurrentManualResume(t *testing.T) {
	ctx := context.Background()
	tx := testEntTx(t)
	repo := newAccountRepositoryWithSQL(tx.Client(), tx, nil)
	limitedAt := time.Now().UTC().Truncate(time.Microsecond)
	account := mustCreateAccount(t, tx.Client(), &service.Account{
		Name: "quota-recovery-manual-resume", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
		Status: service.StatusActive, Schedulable: true,
		Credentials:   map[string]any{"api_key": "test-key", "model_mapping": map[string]any{"gpt-test": "upstream-test"}},
		Extra:         map[string]any{"operator_setting": "keep"},
		RateLimitedAt: &limitedAt,
	})
	require.NoError(t, repo.SetSchedulable(ctx, account.ID, false))

	observed, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)
	require.False(t, observed.Schedulable)

	require.NoError(t, repo.SetSchedulable(ctx, account.ID, true))

	applied, err := repo.ApplyQuotaRecoveryMutation(ctx, service.QuotaRecoveryMutation{
		Target: observed, Identity: observed,
		ClearQuotaExhaustion:     true,
		ExpectedQuotaExhaustedAt: observed.RateLimitedAt,
		ExtraUpdates:             map[string]any{"quota_recovery_snapshot": map[string]any{"available": true}},
	})
	require.NoError(t, err)
	require.True(t, applied)

	got, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)
	require.Equal(t, service.StatusActive, got.Status)
	require.True(t, got.Schedulable, "a manual resume during the probe must survive the CAS update")
	require.Nil(t, got.RateLimitedAt)
	require.Nil(t, got.RateLimitResetAt)
	require.Equal(t, "keep", got.Extra["operator_setting"])
	require.Equal(t, observed.Credentials, got.Credentials, "model mapping and credential identity must remain byte-equivalent")
}

func TestListQuotaRecoveryAccountPageIncludesBalanceAndConnectivityCandidates(t *testing.T) {
	ctx := context.Background()
	tx := testEntTx(t)
	repo := newAccountRepositoryWithSQL(tx.Client(), tx, nil)

	now := time.Now().UTC().Truncate(time.Second)
	resetAt := now.Add(time.Hour)
	create := func(name, platform, accountType string, credentials map[string]any, schedulable, blocked bool) *service.Account {
		account := &service.Account{
			Name: name, Platform: platform, Type: accountType, Status: service.StatusActive,
			Schedulable: schedulable, Credentials: credentials,
		}
		if blocked {
			account.RateLimitedAt = &now
			account.RateLimitResetAt = &resetAt
		}
		created := mustCreateAccount(t, tx.Client(), account)
		if !schedulable {
			require.NoError(t, repo.SetSchedulable(ctx, created.ID, false))
			created.Schedulable = false
		}
		return created
	}

	want := []*service.Account{
		create("quota-scan-anthropic-oauth", service.PlatformAnthropic, service.AccountTypeOAuth, map[string]any{"access_token": "oauth"}, false, false),
		create("quota-scan-openai-oauth", service.PlatformOpenAI, service.AccountTypeOAuth, map[string]any{}, false, false),
		create("quota-scan-antigravity-oauth", service.PlatformAntigravity, service.AccountTypeOAuth, map[string]any{"access_token": "oauth"}, false, false),
		create("quota-scan-grok-oauth", service.PlatformGrok, service.AccountTypeOAuth, map[string]any{}, false, false),
		create("quota-scan-setup-token", service.PlatformAnthropic, service.AccountTypeSetupToken, map[string]any{"access_token": "setup"}, true, true),
		create("quota-scan-gemini-oauth", service.PlatformGemini, service.AccountTypeOAuth, map[string]any{"refresh_token": "refresh"}, true, true),
		create("quota-scan-gemini-service-account", service.PlatformGemini, service.AccountTypeServiceAccount, map[string]any{"service_account_json": `{}`}, true, true),
		create("quota-scan-bedrock", service.PlatformAnthropic, service.AccountTypeBedrock, map[string]any{"auth_mode": "apikey", "api_key": "key"}, true, true),
		create("quota-scan-antigravity-upstream", service.PlatformAntigravity, service.AccountTypeUpstream, map[string]any{"base_url": "https://relay.example.com", "api_key": "key"}, true, true),
	}
	paymentError := create("quota-scan-payment-error", service.PlatformOpenAI, service.AccountTypeAPIKey, map[string]any{"api_key": "key"}, false, false)
	_, err := tx.ExecContext(ctx, "UPDATE accounts SET status = $2, error_message = $3 WHERE id = $1",
		paymentError.ID, service.StatusError, service.QuotaRecoveryPaymentErrorPrefix+" insufficient balance")
	require.NoError(t, err)
	want = append(want, paymentError)

	authError := create("quota-scan-auth-error", service.PlatformOpenAI, service.AccountTypeAPIKey, map[string]any{"api_key": "key"}, false, false)
	_, err = tx.ExecContext(ctx, "UPDATE accounts SET status = $2, error_message = $3 WHERE id = $1",
		authError.ID, service.StatusError, "Authentication failed (401): invalid API key")
	require.NoError(t, err)
	setupUnblocked := create("quota-scan-setup-unblocked", service.PlatformAnthropic, service.AccountTypeSetupToken, map[string]any{"access_token": "setup"}, true, false)
	geminiUnblocked := create("quota-scan-gemini-unblocked", service.PlatformGemini, service.AccountTypeAPIKey, map[string]any{"api_key": "key"}, true, false)
	manualTimedRateLimit := create("quota-scan-manual-timed-rate-limit", service.PlatformGemini, service.AccountTypeAPIKey, map[string]any{"api_key": "key"}, false, true)
	want = append(want, manualTimedRateLimit)
	manualQuotaExhausted := create("quota-scan-manual-quota-exhausted", service.PlatformGemini, service.AccountTypeAPIKey, map[string]any{"api_key": "key"}, false, false)
	_, err = tx.ExecContext(ctx, "UPDATE accounts SET rate_limited_at = $2, rate_limit_reset_at = NULL WHERE id = $1", manualQuotaExhausted.ID, now)
	require.NoError(t, err)
	want = append(want, manualQuotaExhausted)

	page, err := repo.ListQuotaRecoveryAccountPage(ctx, service.QuotaRecoveryAccountPageOptions{Limit: 100})
	require.NoError(t, err)
	require.NotNil(t, page)
	gotIDs := make(map[int64]struct{}, len(page.Accounts))
	for i := range page.Accounts {
		gotIDs[page.Accounts[i].ID] = struct{}{}
	}
	for _, account := range want {
		require.Contains(t, gotIDs, account.ID, account.Name)
	}
	for _, account := range []*service.Account{setupUnblocked, geminiUnblocked, authError} {
		require.NotContains(t, gotIDs, account.ID, account.Name)
	}
}
