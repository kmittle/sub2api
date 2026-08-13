package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	credentialFileLimit = 1 << 20
	refreshLockStale    = 15 * time.Second
	refreshLockPoll     = 500 * time.Millisecond
	refreshLockTouch    = 2 * time.Second
)

type credentialToken struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresAt    int64  `json:"expires_at"`
	Scope        string `json:"scope"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int64  `json:"expires_in"`
}

type credentialState struct {
	Token credentialToken
	Raw   map[string]json.RawMessage
}

type tokenManager struct {
	credentialsFile string
	refreshLock     string
	tokenURL        string
	clientID        string
	identityHeaders http.Header
	client          *http.Client
	now             func() time.Time
	mu              sync.Mutex
}

func newTokenManager(cfg relayConfig, client *http.Client, identityHeaders http.Header) *tokenManager {
	return &tokenManager{
		credentialsFile: cfg.CredentialsFile,
		refreshLock:     cfg.RefreshLockTarget,
		tokenURL:        cfg.OAuthTokenURL,
		clientID:        cfg.OAuthClientID,
		identityHeaders: identityHeaders.Clone(),
		client:          client,
		now:             time.Now,
	}
}

func (m *tokenManager) Check() error {
	_, err := m.load()
	return err
}

func (m *tokenManager) AccessToken(ctx context.Context, force bool) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	initial, err := m.load()
	if err != nil {
		return "", err
	}
	if !force && !m.shouldRefresh(initial.Token) {
		return initial.Token.AccessToken, nil
	}

	release, err := acquireFileLock(ctx, m.refreshLock)
	if err != nil {
		return "", fmt.Errorf("acquire OAuth refresh lock: %w", err)
	}
	defer release()

	afterLock, err := m.load()
	if err != nil {
		return "", err
	}
	if !force && !m.shouldRefresh(afterLock.Token) {
		return afterLock.Token.AccessToken, nil
	}
	if force && tokenChanged(initial.Token, afterLock.Token) && !m.shouldRefresh(afterLock.Token) {
		return afterLock.Token.AccessToken, nil
	}
	if strings.TrimSpace(afterLock.Token.RefreshToken) == "" {
		return "", errors.New("KIMI OAuth refresh token is missing; re-login is required")
	}

	// Once refresh starts it must finish independently of the downstream
	// request. KIMI rotates refresh tokens, so cancelling after the upstream
	// accepted the old token but before the new token was persisted could make
	// the account unrecoverable without an interactive login.
	refreshCtx, cancelRefresh := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancelRefresh()
	refreshed, err := m.refresh(refreshCtx, afterLock.Token.RefreshToken)
	if err != nil {
		return "", err
	}
	afterLock.Token = refreshed
	if err := writeCredentialStateAtomic(m.credentialsFile, afterLock); err != nil {
		return "", fmt.Errorf("persist refreshed KIMI OAuth credentials: %w", err)
	}
	return refreshed.AccessToken, nil
}

func (m *tokenManager) shouldRefresh(token credentialToken) bool {
	if token.ExpiresAt == 0 {
		return false
	}
	threshold := int64(300)
	if half := token.ExpiresIn / 2; half > threshold {
		threshold = half
	}
	return token.ExpiresAt-m.now().Unix() < threshold
}

func (m *tokenManager) load() (credentialState, error) {
	file, err := os.Open(m.credentialsFile)
	if err != nil {
		return credentialState{}, fmt.Errorf("open KIMI OAuth credentials: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, credentialFileLimit+1))
	if err != nil {
		return credentialState{}, fmt.Errorf("read KIMI OAuth credentials: %w", err)
	}
	if len(data) > credentialFileLimit {
		return credentialState{}, errors.New("KIMI OAuth credentials file is unexpectedly large")
	}
	state := credentialState{}
	if err := json.Unmarshal(data, &state.Token); err != nil {
		return credentialState{}, fmt.Errorf("parse KIMI OAuth credentials: %w", err)
	}
	if err := json.Unmarshal(data, &state.Raw); err != nil {
		return credentialState{}, fmt.Errorf("parse raw KIMI OAuth credentials: %w", err)
	}
	if strings.TrimSpace(state.Token.AccessToken) == "" || strings.TrimSpace(state.Token.RefreshToken) == "" {
		return credentialState{}, errors.New("KIMI OAuth credentials are incomplete; re-login is required")
	}
	if state.Token.ExpiresAt < 0 || state.Token.ExpiresIn < 0 {
		return credentialState{}, errors.New("KIMI OAuth credentials contain an invalid expiry")
	}
	return state, nil
}

func (m *tokenManager) refresh(ctx context.Context, refreshToken string) (credentialToken, error) {
	form := url.Values{
		"client_id":     {m.clientID},
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshToken},
	}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(time.Duration(1<<(attempt-1)) * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return credentialToken{}, ctx.Err()
			case <-timer.C:
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.tokenURL, strings.NewReader(form.Encode()))
		if err != nil {
			return credentialToken{}, fmt.Errorf("build KIMI OAuth refresh request: %w", err)
		}
		copyHeaders(req.Header, m.identityHeaders)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Accept", "application/json")

		resp, err := m.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("KIMI OAuth refresh transport failed: %w", err)
			continue
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, credentialFileLimit+1))
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("read KIMI OAuth refresh response: %w", readErr)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("KIMI OAuth refresh returned HTTP %d", resp.StatusCode)
			if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusBadRequest {
				return credentialToken{}, lastErr
			}
			if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < http.StatusInternalServerError {
				return credentialToken{}, lastErr
			}
			continue
		}

		var token credentialToken
		if err := json.Unmarshal(body, &token); err != nil {
			return credentialToken{}, fmt.Errorf("parse KIMI OAuth refresh response: %w", err)
		}
		if token.AccessToken == "" || token.RefreshToken == "" || token.ExpiresIn <= 0 {
			return credentialToken{}, errors.New("KIMI OAuth refresh response is incomplete")
		}
		if token.TokenType == "" {
			token.TokenType = "Bearer"
		}
		token.ExpiresAt = m.now().Unix() + token.ExpiresIn
		return token, nil
	}
	return credentialToken{}, lastErr
}

func tokenChanged(a, b credentialToken) bool {
	return a.AccessToken != b.AccessToken || a.RefreshToken != b.RefreshToken || a.ExpiresAt != b.ExpiresAt
}

func writeCredentialStateAtomic(path string, state credentialState) error {
	if state.Raw == nil {
		state.Raw = make(map[string]json.RawMessage)
	}
	values := map[string]any{
		"access_token":  state.Token.AccessToken,
		"refresh_token": state.Token.RefreshToken,
		"expires_at":    state.Token.ExpiresAt,
		"scope":         state.Token.Scope,
		"token_type":    state.Token.TokenType,
		"expires_in":    state.Token.ExpiresIn,
	}
	for key, value := range values {
		encoded, err := json.Marshal(value)
		if err != nil {
			return err
		}
		state.Raw[key] = encoded
	}
	data, err := json.MarshalIndent(state.Raw, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	defer cleanup()
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := io.Copy(tmp, bytes.NewReader(data)); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return err
	}
	if directory, err := os.Open(dir); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return nil
}

func acquireFileLock(ctx context.Context, target string) (func(), error) {
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return nil, err
	}
	marker, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	_ = marker.Close()
	lockDir := target + ".lock"
	ticker := time.NewTicker(refreshLockPoll)
	defer ticker.Stop()

	for {
		err := os.Mkdir(lockDir, 0o700)
		if err == nil {
			return holdFileLock(lockDir), nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		if info, statErr := os.Stat(lockDir); statErr == nil && time.Since(info.ModTime()) > refreshLockStale {
			_ = os.Remove(lockDir)
			continue
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func holdFileLock(lockDir string) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(refreshLockTouch)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case now := <-ticker.C:
				_ = os.Chtimes(lockDir, now, now)
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stop)
			<-done
			_ = os.Remove(lockDir)
		})
	}
}

func copyHeaders(dst, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}
