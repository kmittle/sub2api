package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTokenManagerRefreshesAndAtomicallyPersistsRotatedToken(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	credentials := filepath.Join(dir, "credentials", "kimi-code.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(credentials), 0o700))
	require.NoError(t, os.WriteFile(credentials, []byte(`{
  "access_token": "old-access",
  "refresh_token": "old-refresh",
  "expires_at": 100,
  "scope": "openid",
  "token_type": "Bearer",
  "expires_in": 900,
  "preserved": "yes"
}`), 0o600))

	oauth := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "old-refresh", r.FormValue("refresh_token"))
		require.Equal(t, defaultKimiClientID, r.FormValue("client_id"))
		require.Equal(t, "device-id", r.Header.Get("X-Msh-Device-Id"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"new-access","refresh_token":"new-refresh","expires_in":900,"scope":"openid","token_type":"Bearer"}`))
	}))
	defer oauth.Close()

	cfg := relayConfig{
		CredentialsFile:   credentials,
		RefreshLockTarget: filepath.Join(dir, "oauth", "kimi-code"),
		OAuthTokenURL:     oauth.URL,
		OAuthClientID:     defaultKimiClientID,
	}
	manager := newTokenManager(cfg, oauth.Client(), http.Header{"X-Msh-Device-Id": {"device-id"}})
	manager.now = func() time.Time { return time.Unix(1_000, 0) }
	token, err := manager.AccessToken(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, "new-access", token)

	data, err := os.ReadFile(credentials)
	require.NoError(t, err)
	var stored map[string]any
	require.NoError(t, json.Unmarshal(data, &stored))
	require.Equal(t, "new-access", stored["access_token"])
	require.Equal(t, "new-refresh", stored["refresh_token"])
	require.Equal(t, float64(1900), stored["expires_at"])
	require.Equal(t, "yes", stored["preserved"])
	info, err := os.Stat(credentials)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestTokenManagerUsesFreshTokenWithoutRefresh(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	credentials := filepath.Join(dir, "kimi-code.json")
	require.NoError(t, os.WriteFile(credentials, []byte(`{"access_token":"fresh","refresh_token":"refresh","expires_at":2000,"expires_in":900}`), 0o600))

	cfg := relayConfig{
		CredentialsFile:   credentials,
		RefreshLockTarget: filepath.Join(dir, "oauth", "kimi-code"),
		OAuthTokenURL:     "https://auth.invalid/token",
		OAuthClientID:     defaultKimiClientID,
	}
	manager := newTokenManager(cfg, http.DefaultClient, nil)
	manager.now = func() time.Time { return time.Unix(1_000, 0) }
	token, err := manager.AccessToken(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, "fresh", token)
}

func TestReadSecretRejectsShortValue(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "secret")
	require.NoError(t, os.WriteFile(path, []byte(strings.Repeat("x", 12)), 0o600))
	_, err := readSecret(path)
	require.Error(t, err)
}
