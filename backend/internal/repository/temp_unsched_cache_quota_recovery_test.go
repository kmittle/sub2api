//go:build unit

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newTempUnschedCacheForQuotaRecoveryTest(t *testing.T) *tempUnschedCache {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return &tempUnschedCache{rdb: rdb}
}

func TestTempUnschedCache_DeleteIfMatchDeletesExactObservedState(t *testing.T) {
	cache := newTempUnschedCacheForQuotaRecoveryTest(t)
	ctx := context.Background()
	state := &service.TempUnschedState{
		UntilUnix:       time.Now().Add(time.Hour).Unix(),
		TriggeredAtUnix: time.Now().Unix(),
		RuleIndex:       -1,
		ErrorMessage:    "account scheduling threshold reached",
	}
	require.NoError(t, cache.SetTempUnsched(ctx, 41, state))

	deleted, err := cache.DeleteTempUnschedIfMatch(ctx, 41, state)
	require.NoError(t, err)
	require.True(t, deleted)
	got, err := cache.GetTempUnsched(ctx, 41)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestTempUnschedCache_DeleteIfMatchPreservesNewGeneration(t *testing.T) {
	cache := newTempUnschedCacheForQuotaRecoveryTest(t)
	ctx := context.Background()
	oldState := &service.TempUnschedState{
		UntilUnix:       time.Now().Add(time.Hour).Unix(),
		TriggeredAtUnix: time.Now().Unix(),
		RuleIndex:       -1,
		ErrorMessage:    "account scheduling threshold reached",
	}
	newState := &service.TempUnschedState{
		UntilUnix:       oldState.UntilUnix + 3600,
		TriggeredAtUnix: oldState.TriggeredAtUnix + 1,
		StatusCode:      403,
		RuleIndex:       2,
		ErrorMessage:    "new operator rule",
	}
	require.NoError(t, cache.SetTempUnsched(ctx, 42, oldState))
	require.NoError(t, cache.SetTempUnsched(ctx, 42, newState))

	deleted, err := cache.DeleteTempUnschedIfMatch(ctx, 42, oldState)
	require.NoError(t, err)
	require.False(t, deleted)
	got, err := cache.GetTempUnsched(ctx, 42)
	require.NoError(t, err)
	require.Equal(t, newState, got)
}
