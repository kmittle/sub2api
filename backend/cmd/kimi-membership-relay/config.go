package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

const (
	defaultKimiAPIBaseURL  = "https://api.kimi.com/coding/v1"
	defaultKimiOAuthURL    = "https://auth.kimi.com/api/oauth/token"
	defaultKimiClientID    = "17e5f671-d194-4dfb-9706-5516cb48c098"
	defaultKimiCodeVersion = "0.35.0"
)

type relayConfig struct {
	ListenAddress     string
	SecretFile        string
	CredentialsFile   string
	DeviceIDFile      string
	RefreshLockTarget string
	APIBaseURL        string
	OAuthTokenURL     string
	OAuthClientID     string
	UpstreamProxyURL  string
	KimiCodeVersion   string
	MaxRequestBytes   int64
	MaxInFlight       int
}

func configFromEnv() (relayConfig, error) {
	cfg := relayConfig{
		ListenAddress:     envOrDefault("KIMI_RELAY_LISTEN_ADDRESS", "0.0.0.0:8090"),
		SecretFile:        envOrDefault("KIMI_RELAY_SECRET_FILE", "/run/secrets/kimi-relay-key"),
		CredentialsFile:   envOrDefault("KIMI_RELAY_CREDENTIALS_FILE", "/run/kimi/credentials/kimi-code.json"),
		DeviceIDFile:      envOrDefault("KIMI_RELAY_DEVICE_ID_FILE", "/run/kimi/device_id"),
		RefreshLockTarget: envOrDefault("KIMI_RELAY_REFRESH_LOCK_TARGET", "/run/kimi/oauth/kimi-code"),
		APIBaseURL:        envOrDefault("KIMI_RELAY_API_BASE_URL", defaultKimiAPIBaseURL),
		OAuthTokenURL:     envOrDefault("KIMI_RELAY_OAUTH_TOKEN_URL", defaultKimiOAuthURL),
		OAuthClientID:     envOrDefault("KIMI_RELAY_OAUTH_CLIENT_ID", defaultKimiClientID),
		UpstreamProxyURL:  strings.TrimSpace(os.Getenv("KIMI_RELAY_UPSTREAM_PROXY")),
		KimiCodeVersion:   envOrDefault("KIMI_RELAY_KIMI_CODE_VERSION", defaultKimiCodeVersion),
		MaxRequestBytes:   32 << 20,
		MaxInFlight:       2,
	}

	var err error
	if raw := strings.TrimSpace(os.Getenv("KIMI_RELAY_MAX_REQUEST_BYTES")); raw != "" {
		cfg.MaxRequestBytes, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return relayConfig{}, fmt.Errorf("parse KIMI_RELAY_MAX_REQUEST_BYTES: %w", err)
		}
	}
	if raw := strings.TrimSpace(os.Getenv("KIMI_RELAY_MAX_IN_FLIGHT")); raw != "" {
		cfg.MaxInFlight, err = strconv.Atoi(raw)
		if err != nil {
			return relayConfig{}, fmt.Errorf("parse KIMI_RELAY_MAX_IN_FLIGHT: %w", err)
		}
	}
	if err := cfg.validate(); err != nil {
		return relayConfig{}, err
	}
	return cfg, nil
}

func (c relayConfig) validate() error {
	if strings.TrimSpace(c.ListenAddress) == "" {
		return errors.New("KIMI_RELAY_LISTEN_ADDRESS is required")
	}
	for name, value := range map[string]string{
		"KIMI_RELAY_SECRET_FILE":         c.SecretFile,
		"KIMI_RELAY_CREDENTIALS_FILE":    c.CredentialsFile,
		"KIMI_RELAY_DEVICE_ID_FILE":      c.DeviceIDFile,
		"KIMI_RELAY_REFRESH_LOCK_TARGET": c.RefreshLockTarget,
		"KIMI_RELAY_OAUTH_CLIENT_ID":     c.OAuthClientID,
		"KIMI_RELAY_KIMI_CODE_VERSION":   c.KimiCodeVersion,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if c.MaxRequestBytes < 1 || c.MaxRequestBytes > 256<<20 {
		return errors.New("KIMI_RELAY_MAX_REQUEST_BYTES must be between 1 and 268435456")
	}
	if c.MaxInFlight < 1 || c.MaxInFlight > 64 {
		return errors.New("KIMI_RELAY_MAX_IN_FLIGHT must be between 1 and 64")
	}
	if err := validateHTTPSURL("KIMI_RELAY_API_BASE_URL", c.APIBaseURL); err != nil {
		return err
	}
	if err := validateHTTPSURL("KIMI_RELAY_OAUTH_TOKEN_URL", c.OAuthTokenURL); err != nil {
		return err
	}
	if c.UpstreamProxyURL == "" {
		return errors.New("KIMI_RELAY_UPSTREAM_PROXY is required; direct upstream access is disabled")
	}
	proxyURL, err := url.Parse(c.UpstreamProxyURL)
	if err != nil || proxyURL.Host == "" {
		return errors.New("KIMI_RELAY_UPSTREAM_PROXY must be an absolute proxy URL")
	}
	switch strings.ToLower(proxyURL.Scheme) {
	case "http", "https", "socks5", "socks5h":
	default:
		return errors.New("KIMI_RELAY_UPSTREAM_PROXY uses an unsupported scheme")
	}
	return nil
}

func validateHTTPSURL(name, raw string) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" || !strings.EqualFold(parsed.Scheme, "https") {
		return fmt.Errorf("%s must be an absolute HTTPS URL", name)
	}
	if parsed.User != nil {
		return fmt.Errorf("%s must not contain user information", name)
	}
	if parsed.Fragment != "" {
		return fmt.Errorf("%s must not contain a fragment", name)
	}
	return nil
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
