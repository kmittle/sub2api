//go:build unit

package service

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type quotaExhaustionAccountRepoStub struct {
	rateLimitAccountRepoStub
	setRateLimitedCalls int
	lastRateLimitedID   int64
	lastResetAt         time.Time
}

func (r *quotaExhaustionAccountRepoStub) SetRateLimited(_ context.Context, id int64, resetAt time.Time) error {
	r.setRateLimitedCalls++
	r.lastRateLimitedID = id
	r.lastResetAt = resetAt
	return nil
}

func TestRateLimitServiceQuotaExhaustionPreservesAdministratorState(t *testing.T) {
	tests := []struct {
		name       string
		platform   string
		statusCode int
		body       []byte
	}{
		{
			name:       "Anthropic credit balance 400",
			platform:   PlatformAnthropic,
			statusCode: http.StatusBadRequest,
			body:       []byte(`{"error":{"message":"Your credit balance is too low to access the Anthropic API"}}`),
		},
		{
			name:       "OpenAI compatible payment required 402",
			platform:   PlatformOpenAI,
			statusCode: http.StatusPaymentRequired,
			body:       []byte(`{"error":{"message":"insufficient balance"}}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &quotaExhaustionAccountRepoStub{}
			blocker := &runtimeBlockRecorder{}
			svc := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
			svc.SetAccountRuntimeBlocker(blocker)
			account := &Account{
				ID: 901, Platform: tt.platform, Type: AccountTypeAPIKey,
				Status: StatusActive, Schedulable: false,
			}

			shouldDisable := svc.HandleUpstreamError(
				context.Background(), account, tt.statusCode, http.Header{}, tt.body,
			)

			require.True(t, shouldDisable)
			require.Zero(t, repo.setErrorCalls, "quota exhaustion must not write status or schedulable")
			require.Equal(t, 1, repo.setRateLimitedCalls)
			require.Equal(t, account.ID, repo.lastRateLimitedID)
			require.True(t, repo.lastResetAt.IsZero(), "a zero reset persists an indefinite quota block")
			require.Equal(t, StatusActive, account.Status)
			require.False(t, account.Schedulable)
			require.Equal(t, []string{"quota_exhausted"}, blocker.reasons)
		})
	}
}
