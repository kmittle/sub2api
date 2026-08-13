package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type staticTokenProvider struct {
	mu         sync.Mutex
	forceCalls []bool
	checkErr   error
	token      string
}

func (p *staticTokenProvider) Check() error { return p.checkErr }

func (p *staticTokenProvider) AccessToken(_ context.Context, force bool) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.forceCalls = append(p.forceCalls, force)
	return p.token, nil
}

func testRelayConfig(apiBaseURL string) relayConfig {
	return relayConfig{
		APIBaseURL:      apiBaseURL,
		MaxRequestBytes: 1 << 20,
		MaxInFlight:     2,
	}
}

func TestNormalizeKimiChatRequest(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantEffort string
		wantErr    bool
	}{
		{name: "flat effort", body: `{"model":"gpt-5.6-sol","messages":[],"reasoning_effort":"max"}`, wantEffort: "max"},
		{name: "responses shape", body: `{"model":"claude-fable","messages":[],"reasoning":{"effort":"high"}}`, wantEffort: "high"},
		{name: "native thinking", body: `{"model":"anything","messages":[],"thinking":{"type":"enabled","effort":"low","keep":"all"}}`, wantEffort: "low"},
		{name: "no effort", body: `{"model":"anything","messages":[]}`},
		{name: "invalid effort", body: `{"model":"anything","messages":[],"reasoning_effort":"medium"}`, wantErr: true},
		{name: "trailing JSON", body: `{"model":"anything"} {}`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := normalizeKimiChatRequest([]byte(tt.body))
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			var got map[string]any
			require.NoError(t, json.Unmarshal(body, &got))
			require.Equal(t, "k3", got["model"])
			require.NotContains(t, got, "reasoning_effort")
			require.NotContains(t, got, "reasoning")
			if tt.wantEffort == "" {
				require.NotContains(t, got, "thinking")
				return
			}
			thinking := got["thinking"].(map[string]any)
			require.Equal(t, "enabled", thinking["type"])
			require.Equal(t, tt.wantEffort, thinking["effort"])
			if tt.name == "native thinking" {
				require.Equal(t, "all", thinking["keep"])
			}
		})
	}
}

func TestRelayRequiresSecretAndForcesK3Effort(t *testing.T) {
	var upstreamBody []byte
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/coding/v1/chat/completions", r.URL.Path)
		require.Equal(t, "Bearer membership-token", r.Header.Get("Authorization"))
		require.Equal(t, "device-id", r.Header.Get("X-Msh-Device-Id"))
		upstreamBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "data: {\"choices\":[]}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()

	provider := &staticTokenProvider{token: "membership-token"}
	secret := strings.Repeat("s", 40)
	identity := http.Header{"X-Msh-Device-Id": {"device-id"}}
	relay := newRelayServer(testRelayConfig(upstream.URL+"/coding/v1"), []byte(secret), provider, upstream.Client(), identity)

	unauthorized := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"x","messages":[]}`))
	unauthorizedRecorder := httptest.NewRecorder()
	relay.ServeHTTP(unauthorizedRecorder, unauthorized)
	require.Equal(t, http.StatusUnauthorized, unauthorizedRecorder.Code)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gpt-5.6-sol","messages":[],"reasoning_effort":"max","stream":true}`))
	req.Header.Set("Authorization", "Bearer "+secret)
	req.Header.Set("Accept", "text/event-stream")
	recorder := httptest.NewRecorder()
	relay.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(), "[DONE]")
	var got map[string]any
	require.NoError(t, json.Unmarshal(upstreamBody, &got))
	require.Equal(t, "k3", got["model"])
	require.Equal(t, "max", got["thinking"].(map[string]any)["effort"])
	provider.mu.Lock()
	require.Equal(t, []bool{false}, provider.forceCalls)
	provider.mu.Unlock()
}

func TestRelayRefreshesOnceAfterUnauthorized(t *testing.T) {
	var calls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":[{"id":"k3"}]}`)
	}))
	defer upstream.Close()

	provider := &staticTokenProvider{token: "membership-token"}
	secret := strings.Repeat("s", 40)
	relay := newRelayServer(testRelayConfig(upstream.URL+"/coding/v1"), []byte(secret), provider, upstream.Client(), nil)
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+secret)
	recorder := httptest.NewRecorder()
	relay.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `{"data":[{"id":"k3"}]}`, recorder.Body.String())
	provider.mu.Lock()
	require.Equal(t, []bool{false, true}, provider.forceCalls)
	provider.mu.Unlock()
}

func TestRelayPassesThroughUpstreamErrorWithoutAppendingRelayError(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"message":"quota"}}`)
	}))
	defer upstream.Close()

	provider := &staticTokenProvider{token: "membership-token"}
	secret := strings.Repeat("s", 40)
	relay := newRelayServer(testRelayConfig(upstream.URL+"/coding/v1"), []byte(secret), provider, upstream.Client(), nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(`{"model":"x","messages":[]}`))
	req.Header.Set("Authorization", "Bearer "+secret)
	recorder := httptest.NewRecorder()
	relay.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusTooManyRequests, recorder.Code)
	require.JSONEq(t, `{"error":{"message":"quota"}}`, recorder.Body.String())
}
