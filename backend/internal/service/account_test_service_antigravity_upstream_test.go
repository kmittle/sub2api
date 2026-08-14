//go:build unit

package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type antigravityUpstreamProbeHTTPStub struct {
	response *http.Response
	request  *http.Request
	calls    int
}

func (s *antigravityUpstreamProbeHTTPStub) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	s.calls++
	s.request = req
	if s.response == nil {
		return nil, fmt.Errorf("missing mocked response")
	}
	return s.response, nil
}

func (*antigravityUpstreamProbeHTTPStub) DoWithTLS(*http.Request, string, int64, int, *tlsfingerprint.Profile) (*http.Response, error) {
	return nil, fmt.Errorf("unexpected TLS upstream call")
}

type antigravityUpstreamProbeRepo struct {
	mockAccountRepoForGemini
	rateLimitWrites int
	modelWrites     int
	errorWrites     int
}

func (r *antigravityUpstreamProbeRepo) SetRateLimited(context.Context, int64, time.Time) error {
	r.rateLimitWrites++
	return nil
}

func (r *antigravityUpstreamProbeRepo) SetModelRateLimit(context.Context, int64, string, time.Time, ...string) error {
	r.modelWrites++
	return nil
}

func (r *antigravityUpstreamProbeRepo) SetError(context.Context, int64, string) error {
	r.errorWrites++
	return nil
}

func TestAntigravityUpstreamBackgroundProbeUsesConfiguredRelayDirectly(t *testing.T) {
	t.Parallel()

	account := &Account{
		ID: 201, Platform: PlatformAntigravity, Type: AccountTypeUpstream, Status: StatusActive,
		Concurrency: 1,
		Credentials: map[string]any{
			"base_url": "https://relay.example.com/root/",
			"api_key":  "relay-secret",
		},
	}
	repo := &antigravityUpstreamProbeRepo{}
	repo.accountsByID = map[int64]*Account{account.ID: account}
	upstream := &antigravityUpstreamProbeHTTPStub{response: &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("data: {\"type\":\"message_stop\"}\n\n")),
	}}
	svc := NewAccountTestService(repo, nil, nil, nil, nil, upstream, &config.Config{}, nil)

	result, err := svc.RunTestBackground(withQuotaRecoveryReadOnlyProbe(context.Background()), account.ID, "claude-sonnet-4-5")
	require.NoError(t, err)
	require.Equal(t, "success", result.Status)
	require.Equal(t, 1, upstream.calls)
	require.NotNil(t, upstream.request)
	require.Equal(t, "https://relay.example.com/root/v1/messages", upstream.request.URL.String())
	require.Equal(t, "Bearer relay-secret", upstream.request.Header.Get("Authorization"))
	require.Equal(t, "relay-secret", upstream.request.Header.Get("x-api-key"))
	require.Equal(t, "2023-06-01", upstream.request.Header.Get("anthropic-version"))
	body, err := io.ReadAll(upstream.request.Body)
	require.NoError(t, err)
	require.Equal(t, "claude-sonnet-4-5", gjson.GetBytes(body, "model").String())
	require.Zero(t, repo.rateLimitWrites)
	require.Zero(t, repo.modelWrites)
	require.Zero(t, repo.errorWrites)
}

func TestAntigravityUpstreamReadOnlyProbeFailureDoesNotWriteSchedulingState(t *testing.T) {
	t.Parallel()

	account := &Account{
		ID: 202, Platform: PlatformAntigravity, Type: AccountTypeUpstream, Status: StatusActive,
		Concurrency: 1,
		Credentials: map[string]any{
			"base_url": "https://relay.example.com",
			"api_key":  "relay-secret",
		},
	}
	repo := &antigravityUpstreamProbeRepo{}
	repo.accountsByID = map[int64]*Account{account.ID: account}
	upstream := &antigravityUpstreamProbeHTTPStub{response: &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"quota exhausted"}}`)),
	}}
	svc := NewAccountTestService(repo, nil, nil, nil, nil, upstream, &config.Config{}, nil)

	result, err := svc.RunTestBackground(withQuotaRecoveryReadOnlyProbe(context.Background()), account.ID, "")
	require.NoError(t, err)
	require.Equal(t, "failed", result.Status)
	require.Contains(t, result.ErrorMessage, "429")
	require.Equal(t, 1, upstream.calls)
	require.Zero(t, repo.rateLimitWrites)
	require.Zero(t, repo.modelWrites)
	require.Zero(t, repo.errorWrites)
}
