package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		slog.Error("kimi membership relay stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}
	secret, err := readSecret(cfg.SecretFile)
	if err != nil {
		return err
	}
	identity, err := kimiIdentityHeaders(cfg)
	if err != nil {
		return err
	}
	proxyURL, err := parseProxyURL(cfg.UpstreamProxyURL)
	if err != nil {
		return fmt.Errorf("parse KIMI upstream proxy: %w", err)
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyURL(proxyURL),
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          16,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 5 * time.Minute,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	client := &http.Client{Transport: transport}
	tokens := newTokenManager(cfg, client, identity)
	if err := tokens.Check(); err != nil {
		return err
	}

	server := &http.Server{
		Addr:              cfg.ListenAddress,
		Handler:           newRelayServer(cfg, secret, tokens, client, identity),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       2 * time.Minute,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    32 << 10,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	slog.Info("kimi membership relay listening", "address", cfg.ListenAddress, "max_in_flight", cfg.MaxInFlight, "max_request_bytes", cfg.MaxRequestBytes)
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func readSecret(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read relay secret: %w", err)
	}
	secret := []byte(strings.TrimSpace(string(data)))
	if len(secret) < 32 {
		return nil, errors.New("relay secret must contain at least 32 characters")
	}
	return secret, nil
}

func kimiIdentityHeaders(cfg relayConfig) (http.Header, error) {
	deviceID, err := os.ReadFile(cfg.DeviceIDFile)
	if err != nil {
		return nil, fmt.Errorf("read KIMI device identity: %w", err)
	}
	device := strings.TrimSpace(string(deviceID))
	if device == "" || len(device) > 256 {
		return nil, errors.New("KIMI device identity is invalid")
	}
	version := strings.TrimSpace(cfg.KimiCodeVersion)
	return http.Header{
		"User-Agent":         {"kimi-code-cli/" + version},
		"X-Msh-Platform":     {"kimi_code_cli"},
		"X-Msh-Version":      {version},
		"X-Msh-Device-Name":  {"sub2api-kimi-relay"},
		"X-Msh-Device-Model": {runtime.GOARCH},
		"X-Msh-Os-Version":   {runtime.GOOS},
		"X-Msh-Device-Id":    {device},
	}, nil
}
