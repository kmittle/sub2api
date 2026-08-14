//go:build unit

package repository

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestClaimQuotaRecoverySlotUsesCanonicalUTCValue(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	slot := time.Date(2026, 8, 14, 12, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO settings (key, value, updated_at)")).
		WithArgs(service.QuotaRecoverySlotSettingKey, "2026-08-14T04:00:00Z").
		WillReturnResult(sqlmock.NewResult(0, 1))

	repo := newAccountRepositoryWithSQL(nil, db, nil)
	claimed, err := repo.ClaimQuotaRecoverySlot(context.Background(), slot)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReleaseQuotaRecoverySlotDeletesOnlyTheExactClaim(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		affected int64
		want     bool
	}{
		{name: "matching slot", affected: 1, want: true},
		{name: "newer or operator value", affected: 0, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })

			slot := time.Date(2026, 8, 14, 4, 0, 0, 0, time.UTC)
			mock.ExpectExec(regexp.QuoteMeta("DELETE FROM settings WHERE key = $1 AND value = $2")).
				WithArgs(service.QuotaRecoverySlotSettingKey, slot.Format(time.RFC3339)).
				WillReturnResult(sqlmock.NewResult(0, tt.affected))

			repo := newAccountRepositoryWithSQL(nil, db, nil)
			released, err := repo.ReleaseQuotaRecoverySlot(context.Background(), slot)
			require.NoError(t, err)
			require.Equal(t, tt.want, released)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestApplyQuotaRecoveryMutationRejectsUnsafeClearRequestsBeforeSQL(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	repo := newAccountRepositoryWithSQL(nil, db, nil)
	account := &service.Account{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth}
	until := time.Now().Add(time.Hour)

	_, err = repo.ApplyQuotaRecoveryMutation(context.Background(), service.QuotaRecoveryMutation{
		Target: account, Identity: account,
		ClearThresholdBlock: true,
		ExpectedTempUntil:   &until,
		ExpectedTempReason:  "plain operator reason",
	})
	require.ErrorContains(t, err, "non-threshold")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestApplyQuotaRecoveryMutationRejectsNonBalanceAccountErrorBeforeSQL(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	repo := newAccountRepositoryWithSQL(nil, db, nil)
	account := &service.Account{
		ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
		Status: service.StatusError, ErrorMessage: "Authentication failed (401): invalid API key",
	}

	_, err = repo.ApplyQuotaRecoveryMutation(context.Background(), service.QuotaRecoveryMutation{
		Target: account, Identity: account, ClearQuotaError: true,
	})
	require.ErrorContains(t, err, "non-balance")
	require.NoError(t, mock.ExpectationsWereMet())
}
