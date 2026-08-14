package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const tempUnschedPrefix = "temp_unsched:account:"

var tempUnschedSetScript = redis.NewScript(`
	local key = KEYS[1]
	local new_until = tonumber(ARGV[1])
	local new_value = ARGV[2]
	local new_ttl = tonumber(ARGV[3])

	local existing = redis.call('GET', key)
	if existing then
		local ok, existing_data = pcall(cjson.decode, existing)
		if ok and existing_data and existing_data.until_unix then
			local existing_until = tonumber(existing_data.until_unix)
			if existing_until and new_until <= existing_until then
				return 0
			end
		end
	end

	redis.call('SET', key, new_value, 'EX', new_ttl)
	return 1
`)

var tempUnschedDeleteIfMatchScript = redis.NewScript(`
	local key = KEYS[1]
	local existing = redis.call('GET', key)
	if not existing then
		return 0
	end
	local ok, state = pcall(cjson.decode, existing)
	if not ok or not state then
		return 0
	end
	if tonumber(state.until_unix or 0) ~= tonumber(ARGV[1]) or
		tonumber(state.triggered_at_unix or 0) ~= tonumber(ARGV[2]) or
		tonumber(state.status_code or 0) ~= tonumber(ARGV[3]) or
		tostring(state.matched_keyword or '') ~= ARGV[4] or
		tonumber(state.rule_index or 0) ~= tonumber(ARGV[5]) or
		tostring(state.error_message or '') ~= ARGV[6] or
		tonumber(state.trigger_count or 0) ~= tonumber(ARGV[7]) or
		tonumber(state.trigger_threshold or 0) ~= tonumber(ARGV[8]) or
		tonumber(state.trigger_window_minutes or 0) ~= tonumber(ARGV[9]) then
		return 0
	end
	return redis.call('DEL', key)
`)

type tempUnschedCache struct {
	rdb *redis.Client
}

func NewTempUnschedCache(rdb *redis.Client) service.TempUnschedCache {
	return &tempUnschedCache{rdb: rdb}
}

// SetTempUnsched 设置临时不可调度状态（只延长不缩短）
func (c *tempUnschedCache) SetTempUnsched(ctx context.Context, accountID int64, state *service.TempUnschedState) error {
	key := fmt.Sprintf("%s%d", tempUnschedPrefix, accountID)

	stateJSON, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	ttl := time.Until(time.Unix(state.UntilUnix, 0))
	if ttl <= 0 {
		return nil // 已过期，不设置
	}

	ttlSeconds := int(ttl.Seconds())
	if ttlSeconds < 1 {
		ttlSeconds = 1
	}

	_, err = tempUnschedSetScript.Run(ctx, c.rdb, []string{key}, state.UntilUnix, string(stateJSON), ttlSeconds).Result()
	return err
}

// GetTempUnsched 获取临时不可调度状态
func (c *tempUnschedCache) GetTempUnsched(ctx context.Context, accountID int64) (*service.TempUnschedState, error) {
	key := fmt.Sprintf("%s%d", tempUnschedPrefix, accountID)

	val, err := c.rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var state service.TempUnschedState
	if err := json.Unmarshal([]byte(val), &state); err != nil {
		return nil, fmt.Errorf("unmarshal state: %w", err)
	}

	return &state, nil
}

// DeleteTempUnsched 删除临时不可调度状态
func (c *tempUnschedCache) DeleteTempUnsched(ctx context.Context, accountID int64) error {
	key := fmt.Sprintf("%s%d", tempUnschedPrefix, accountID)
	return c.rdb.Del(ctx, key).Err()
}

// DeleteTempUnschedIfMatch removes only the exact state observed before a
// quota probe. A concurrently written temporary block is left untouched.
func (c *tempUnschedCache) DeleteTempUnschedIfMatch(ctx context.Context, accountID int64, expected *service.TempUnschedState) (bool, error) {
	if expected == nil {
		return false, nil
	}
	key := fmt.Sprintf("%s%d", tempUnschedPrefix, accountID)
	deleted, err := tempUnschedDeleteIfMatchScript.Run(
		ctx,
		c.rdb,
		[]string{key},
		expected.UntilUnix,
		expected.TriggeredAtUnix,
		expected.StatusCode,
		expected.MatchedKeyword,
		expected.RuleIndex,
		expected.ErrorMessage,
		expected.TriggerCount,
		expected.TriggerThreshold,
		expected.TriggerWindowMinutes,
	).Int()
	if err != nil {
		return false, err
	}
	return deleted == 1, nil
}

var _ service.QuotaRecoveryTempUnschedCache = (*tempUnschedCache)(nil)
