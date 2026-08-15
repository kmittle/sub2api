package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

func (r *accountRepository) ClaimQuotaRecoverySlot(ctx context.Context, slot time.Time) (bool, error) {
	if r == nil || r.sql == nil {
		return false, errors.New("quota recovery repository is not configured")
	}
	value := slot.UTC().Format(time.RFC3339)
	result, err := r.sql.ExecContext(ctx, `
		INSERT INTO settings (key, value, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (key) DO UPDATE
		SET value = EXCLUDED.value,
			updated_at = NOW()
		WHERE settings.value <> EXCLUDED.value
			AND (
				settings.value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
				OR settings.value < EXCLUDED.value
			)`,
		service.QuotaRecoverySlotSettingKey,
		value,
	)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected > 0, err
}

// ReleaseQuotaRecoverySlot deletes only the exact slot claimed by the failed
// scan. A newer slot or an operator-written value is left untouched.
func (r *accountRepository) ReleaseQuotaRecoverySlot(ctx context.Context, slot time.Time) (bool, error) {
	if r == nil || r.sql == nil {
		return false, errors.New("quota recovery repository is not configured")
	}
	result, err := r.sql.ExecContext(ctx, `
		DELETE FROM settings
		WHERE key = $1 AND value = $2`,
		service.QuotaRecoverySlotSettingKey,
		slot.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected > 0, err
}

func (r *accountRepository) ListQuotaRecoveryAccountPage(ctx context.Context, options service.QuotaRecoveryAccountPageOptions) (*service.QuotaRecoveryAccountPage, error) {
	if r == nil || r.sql == nil {
		return nil, errors.New("quota recovery repository is not configured")
	}
	if options.Limit <= 0 || options.Limit > 1000 {
		return nil, errors.New("quota recovery page limit must be between 1 and 1000")
	}

	rows, err := r.sql.QueryContext(ctx, `
		SELECT id
		FROM accounts
		WHERE deleted_at IS NULL
			AND id > $1
			AND (auto_pause_on_expired IS NOT TRUE OR expires_at IS NULL OR expires_at > NOW())
			AND COALESCE(extra->>'synthetic_ui_test', 'false') <> 'true'
			AND (
				(
					status = $2
					AND (
						(platform = $3 AND type = $4 AND btrim(COALESCE(credentials->>'access_token', '')) <> '')
						OR (platform = $5 AND type = $4)
						OR (platform = $6 AND type = $4 AND btrim(COALESCE(credentials->>'access_token', '')) <> '')
						OR (platform = $7 AND type = $4)
						OR (
							schedulable IS TRUE
							AND platform IN ($3, $5, $6, $7, $8)
							AND type IN ($4, $9, $10, $11, $12, $13)
							AND (
								(rate_limited_at IS NOT NULL AND rate_limit_reset_at IS NOT NULL)
								OR (temp_unschedulable_until IS NOT NULL AND temp_unschedulable_reason LIKE '%account_scheduling_threshold%')
							)
						)
					)
				)
				OR (
					status = $15
					AND schedulable IS FALSE
					AND (error_message LIKE $16 OR error_message LIKE $17)
					AND platform IN ($3, $5, $6, $7, $8)
					AND type IN ($4, $9, $10, $11, $12, $13)
				)
			)
		ORDER BY id ASC
		LIMIT $14`,
		options.AfterID,
		service.StatusActive,
		service.PlatformAnthropic,
		service.AccountTypeOAuth,
		service.PlatformOpenAI,
		service.PlatformAntigravity,
		service.PlatformGrok,
		service.PlatformGemini,
		service.AccountTypeSetupToken,
		service.AccountTypeAPIKey,
		service.AccountTypeUpstream,
		service.AccountTypeBedrock,
		service.AccountTypeServiceAccount,
		options.Limit+1,
		service.StatusError,
		service.QuotaRecoveryCreditBalanceErrorPrefix+"%",
		service.QuotaRecoveryPaymentErrorPrefix+"%",
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	ids := make([]int64, 0, options.Limit+1)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	hasMore := len(ids) > options.Limit
	if hasMore {
		ids = ids[:options.Limit]
	}
	if len(ids) == 0 {
		return &service.QuotaRecoveryAccountPage{Accounts: []service.Account{}}, nil
	}

	accounts, err := r.GetByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	byID := make(map[int64]*service.Account, len(accounts))
	for _, account := range accounts {
		if account != nil {
			byID[account.ID] = account
		}
	}
	pageAccounts := make([]service.Account, 0, len(accounts))
	for _, id := range ids {
		if account := byID[id]; account != nil {
			pageAccounts = append(pageAccounts, *account)
		}
	}
	return &service.QuotaRecoveryAccountPage{
		Accounts:    pageAccounts,
		NextAfterID: ids[len(ids)-1],
		HasMore:     hasMore,
	}, nil
}

// ApplyQuotaRecoveryMutation merges a display snapshot and clears selected
// quota-derived blocks in one SQL statement. Every recovery predicate is a CAS
// against the pre-probe target and credential identity; a stale observation
// therefore becomes a harmless false result.
func (r *accountRepository) ApplyQuotaRecoveryMutation(ctx context.Context, mutation service.QuotaRecoveryMutation) (bool, error) {
	if r == nil || r.sql == nil || mutation.Target == nil || mutation.Identity == nil {
		return false, errors.New("quota recovery mutation is not configured")
	}
	if mutation.Target.ID <= 0 || mutation.Identity.ID <= 0 {
		return false, errors.New("quota recovery mutation has an invalid account identity")
	}
	if mutation.ClearGlobalRateLimit && (mutation.ExpectedRateLimitedAt == nil || mutation.ExpectedRateLimitResetAt == nil) {
		return false, errors.New("quota recovery global clear is missing its observed generation")
	}
	if mutation.ClearThresholdBlock && (mutation.ExpectedTempUntil == nil || mutation.ExpectedTempReason == "") {
		return false, errors.New("quota recovery threshold clear is missing its observed generation")
	}
	if mutation.ClearThresholdBlock && !service.IsAccountSchedulingThresholdReason(mutation.ExpectedTempReason) {
		return false, errors.New("quota recovery refuses a non-threshold temporary block")
	}
	if mutation.ClearQuotaError && (mutation.Target.Status != service.StatusError || mutation.Target.Schedulable ||
		!service.IsQuotaRecoveryBalanceErrorMessage(mutation.Target.ErrorMessage)) {
		return false, errors.New("quota recovery refuses a non-balance account error")
	}
	targetHasBalanceError := mutation.Target.Status == service.StatusError && !mutation.Target.Schedulable &&
		service.IsQuotaRecoveryBalanceErrorMessage(mutation.Target.ErrorMessage)
	if mutation.Target.Status != service.StatusActive && !targetHasBalanceError {
		return false, errors.New("quota recovery target is not active or balance-exhausted")
	}
	identityOwnsTargetError := mutation.Identity.ID == mutation.Target.ID && targetHasBalanceError
	if mutation.Identity.Status != service.StatusActive && !identityOwnsTargetError {
		return false, errors.New("quota recovery credential identity is not active")
	}
	if mutation.ClearQuotaError && mutation.Identity.ID != mutation.Target.ID && !mutation.Identity.Schedulable {
		return false, errors.New("quota recovery credential identity is not schedulable")
	}

	keys := append([]string{}, mutation.ClearModelRateLimitKeys...)
	sort.Strings(keys)
	for i := 1; i < len(keys); i++ {
		if keys[i] == keys[i-1] {
			return false, fmt.Errorf("duplicate quota recovery model key %q", keys[i])
		}
	}
	for _, key := range keys {
		if _, ok := mutation.ExpectedModelRateLimits[key]; !ok {
			return false, fmt.Errorf("quota recovery model key %q has no observed generation", key)
		}
	}
	if len(mutation.ExpectedModelRateLimits) != len(keys) {
		return false, errors.New("quota recovery model generations do not match the requested keys")
	}

	updatesJSON, err := json.Marshal(normalizeJSONMap(mutation.ExtraUpdates))
	if err != nil {
		return false, err
	}
	targetCredentialsJSON, err := json.Marshal(normalizeJSONMap(mutation.Target.Credentials))
	if err != nil {
		return false, err
	}
	identityCredentialsJSON, err := json.Marshal(normalizeJSONMap(mutation.Identity.Credentials))
	if err != nil {
		return false, err
	}
	expectedModelsJSON, err := json.Marshal(normalizeJSONMap(mutation.ExpectedModelRateLimits))
	if err != nil {
		return false, err
	}
	identityTLSEnabledJSON, err := json.Marshal(mutation.Identity.Extra["enable_tls_fingerprint"])
	if err != nil {
		return false, err
	}
	identityTLSProfileJSON, err := json.Marshal(mutation.Identity.Extra["tls_fingerprint_profile_id"])
	if err != nil {
		return false, err
	}
	targetOveragesJSON, err := json.Marshal(mutation.Target.Extra["allow_overages"])
	if err != nil {
		return false, err
	}

	targetProxyID := nullableInt64(mutation.Target.ProxyID)
	targetParentID := nullableInt64(mutation.Target.ParentAccountID)
	identityProxyID := nullableInt64(mutation.Identity.ProxyID)
	identityParentID := nullableInt64(mutation.Identity.ParentAccountID)
	proxyCaptured := mutation.Identity.ProxyID == nil
	var proxyProtocol, proxyHost, proxyUsername, proxyPassword, proxyStatus string
	var proxyPort int
	if mutation.Identity.ProxyID != nil && mutation.Identity.Proxy != nil && mutation.Identity.Proxy.ID == *mutation.Identity.ProxyID {
		proxyCaptured = true
		proxyProtocol = mutation.Identity.Proxy.Protocol
		proxyHost = mutation.Identity.Proxy.Host
		proxyPort = mutation.Identity.Proxy.Port
		proxyUsername = mutation.Identity.Proxy.Username
		proxyPassword = mutation.Identity.Proxy.Password
		proxyStatus = mutation.Identity.Proxy.Status
	}

	setSessionEnd := mutation.SessionWindowEnd != nil
	requiresSchedulable := !mutation.ClearQuotaError &&
		(mutation.ClearGlobalRateLimit || mutation.ClearThresholdBlock || len(keys) > 0)
	rows, err := r.sql.QueryContext(ctx, `
		WITH candidate AS (
			SELECT a.id,
				COALESCE(a.extra, '{}'::jsonb) || $1::jsonb AS merged_extra
			FROM accounts AS a
			JOIN accounts AS i ON i.id = $13
			WHERE a.id = $7
				AND a.deleted_at IS NULL
				AND i.deleted_at IS NULL
				AND a.platform = $8
				AND a.type = $9
				AND a.credentials = $10::jsonb
				AND a.proxy_id IS NOT DISTINCT FROM $11
				AND a.parent_account_id IS NOT DISTINCT FROM $12
				AND a.quota_dimension = $36
				AND COALESCE(a.extra->'allow_overages', 'null'::jsonb) = $37::jsonb
				AND i.platform = $14
				AND i.type = $15
				AND i.credentials = $16::jsonb
				AND i.proxy_id IS NOT DISTINCT FROM $17
				AND i.parent_account_id IS NOT DISTINCT FROM $18
				AND COALESCE(i.extra->'enable_tls_fingerprint', 'null'::jsonb) = $19::jsonb
				AND COALESCE(i.extra->'tls_fingerprint_profile_id', 'null'::jsonb) = $20::jsonb
				AND (
					$17::bigint IS NULL
					OR ($21::boolean AND EXISTS (
						SELECT 1 FROM proxies AS p
						WHERE p.id = $17
							AND p.deleted_at IS NULL
							AND p.protocol = $22
							AND p.host = $23
							AND p.port = $24
							AND COALESCE(p.username, '') = $25
							AND COALESCE(p.password, '') = $26
							AND p.status = $27
					))
				)
				AND a.status = $34
				AND i.status = $38
				AND (a.auto_pause_on_expired IS NOT TRUE OR a.expires_at IS NULL OR a.expires_at > NOW())
				AND (i.auto_pause_on_expired IS NOT TRUE OR i.expires_at IS NULL OR i.expires_at > NOW())
				AND (NOT $28::boolean OR (a.schedulable IS TRUE AND i.schedulable IS TRUE))
				AND (NOT $39::boolean OR (
					a.schedulable IS FALSE
					AND a.error_message = $40
					AND i.schedulable IS NOT DISTINCT FROM $41
					AND i.error_message = $42
				))
				AND (NOT $4::boolean OR (
					a.rate_limited_at = $29::timestamptz
					AND a.rate_limit_reset_at = $30::timestamptz
				))
				AND (NOT $5::boolean OR (
					a.temp_unschedulable_until = $31::timestamptz
					AND a.temp_unschedulable_reason = $32
				))
				AND (
					cardinality($6::text[]) = 0
					OR NOT EXISTS (
						SELECT 1
						FROM jsonb_each($33::jsonb) AS expected(key, value)
						WHERE (COALESCE(a.extra->'model_rate_limits', '{}'::jsonb)->expected.key) IS DISTINCT FROM expected.value
					)
				)
		), updated AS (
			UPDATE accounts AS a
			SET extra = CASE
					WHEN cardinality($6::text[]) = 0 THEN candidate.merged_extra
					WHEN (COALESCE(candidate.merged_extra->'model_rate_limits', '{}'::jsonb) - $6::text[]) = '{}'::jsonb
						THEN candidate.merged_extra - 'model_rate_limits'
					ELSE jsonb_set(
						candidate.merged_extra,
						'{model_rate_limits}',
						COALESCE(candidate.merged_extra->'model_rate_limits', '{}'::jsonb) - $6::text[],
						true
					)
					END,
				session_window_end = CASE WHEN $3::boolean THEN $2::timestamptz ELSE a.session_window_end END,
				rate_limited_at = CASE WHEN $4::boolean THEN NULL ELSE a.rate_limited_at END,
				rate_limit_reset_at = CASE WHEN $4::boolean THEN NULL ELSE a.rate_limit_reset_at END,
				temp_unschedulable_until = CASE WHEN $5::boolean THEN NULL ELSE a.temp_unschedulable_until END,
				temp_unschedulable_reason = CASE WHEN $5::boolean THEN NULL ELSE a.temp_unschedulable_reason END,
				status = CASE WHEN $39::boolean THEN $43 ELSE a.status END,
				error_message = CASE WHEN $39::boolean THEN '' ELSE a.error_message END,
				schedulable = CASE WHEN $39::boolean THEN TRUE ELSE a.schedulable END,
				updated_at = NOW()
			FROM candidate
			WHERE a.id = candidate.id
			RETURNING a.id
		), outbox AS (
			INSERT INTO scheduler_outbox (event_type, account_id, group_id, payload)
			SELECT $35, id, NULL, NULL FROM updated
			RETURNING account_id
		)
		SELECT id FROM updated`,
		string(updatesJSON),
		mutation.SessionWindowEnd,
		setSessionEnd,
		mutation.ClearGlobalRateLimit,
		mutation.ClearThresholdBlock,
		pq.Array(keys),
		mutation.Target.ID,
		mutation.Target.Platform,
		mutation.Target.Type,
		string(targetCredentialsJSON),
		targetProxyID,
		targetParentID,
		mutation.Identity.ID,
		mutation.Identity.Platform,
		mutation.Identity.Type,
		string(identityCredentialsJSON),
		identityProxyID,
		identityParentID,
		string(identityTLSEnabledJSON),
		string(identityTLSProfileJSON),
		proxyCaptured,
		proxyProtocol,
		proxyHost,
		proxyPort,
		proxyUsername,
		proxyPassword,
		proxyStatus,
		requiresSchedulable,
		mutation.ExpectedRateLimitedAt,
		mutation.ExpectedRateLimitResetAt,
		mutation.ExpectedTempUntil,
		mutation.ExpectedTempReason,
		string(expectedModelsJSON),
		mutation.Target.Status,
		service.SchedulerOutboxEventAccountChanged,
		mutation.Target.QuotaDimensionOrDefault(),
		string(targetOveragesJSON),
		mutation.Identity.Status,
		mutation.ClearQuotaError,
		mutation.Target.ErrorMessage,
		mutation.Identity.Schedulable,
		mutation.Identity.ErrorMessage,
		service.StatusActive,
	)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return false, err
		}
		return false, nil
	}
	var accountID int64
	if err := rows.Scan(&accountID); err != nil {
		return false, err
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	r.syncSchedulerAccountSnapshotDetached(ctx, accountID)
	return true, nil
}

func nullableInt64(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

var _ service.QuotaRecoveryRepository = (*accountRepository)(nil)
var _ sqlExecutor = (*sql.DB)(nil)
