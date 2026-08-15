//go:build integration

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestSetRateLimitedQuotaExhaustionPreservesAdministratorStateAndCannotBeShortened(t *testing.T) {
	ctx := context.Background()
	tx := testEntTx(t)
	repo := newAccountRepositoryWithSQL(tx.Client(), tx, nil)
	account := mustCreateAccount(t, tx.Client(), &service.Account{
		Name: "quota-exhaustion-state", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
		Status: service.StatusActive, Schedulable: false,
		Credentials: map[string]any{"api_key": "test-key"},
	})
	_, err := tx.Client().Account.UpdateOneID(account.ID).SetSchedulable(false).Save(ctx)
	require.NoError(t, err)

	require.NoError(t, repo.SetRateLimited(ctx, account.ID, time.Time{}))
	got, err := repo.GetByID(ctx, account.ID)
	require.NoError(t, err)
	require.Equal(t, service.StatusActive, got.Status)
	require.False(t, got.Schedulable)
	require.NotNil(t, got.RateLimitedAt)
	require.Nil(t, got.RateLimitResetAt)
	require.True(t, got.IsRateLimited())

	require.NoError(t, repo.SetRateLimited(ctx, account.ID, time.Now().Add(time.Minute)))
	got, err = repo.GetByID(ctx, account.ID)
	require.NoError(t, err)
	require.NotNil(t, got.RateLimitedAt)
	require.Nil(t, got.RateLimitResetAt, "a finite cooldown must not weaken an indefinite quota block")
}
