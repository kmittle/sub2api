package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type relayServer struct {
	secret          []byte
	apiBaseURL      string
	maxRequestBytes int64
	identityHeaders http.Header
	tokens          relayTokenProvider
	client          *http.Client
	inFlight        chan struct{}
}

type relayTokenProvider interface {
	Check() error
	AccessToken(context.Context, bool) (string, error)
}

func newRelayServer(cfg relayConfig, secret []byte, tokens relayTokenProvider, client *http.Client, identityHeaders http.Header) *relayServer {
	return &relayServer{
		secret:          append([]byte(nil), secret...),
		apiBaseURL:      strings.TrimRight(cfg.APIBaseURL, "/"),
		maxRequestBytes: cfg.MaxRequestBytes,
		identityHeaders: identityHeaders.Clone(),
		tokens:          tokens,
		client:          client,
		inFlight:        make(chan struct{}, cfg.MaxInFlight),
	}
}

func (s *relayServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tracked := &trackingResponseWriter{ResponseWriter: w}
	w = tracked
	started := time.Now()
	status := http.StatusOK

	if r.Method == http.MethodGet && r.URL.Path == "/health" {
		if err := s.tokens.Check(); err != nil {
			status = http.StatusServiceUnavailable
			writeJSONError(w, status, "credentials_unavailable")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"status":"ok"}`)
		return
	}
	defer func() {
		slog.Info("kimi relay request", "method", r.Method, "path", r.URL.Path, "status", status, "duration_ms", time.Since(started).Milliseconds())
	}()

	if !s.authorized(r.Header.Get("Authorization")) {
		status = http.StatusUnauthorized
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeJSONError(w, status, "unauthorized")
		return
	}

	select {
	case s.inFlight <- struct{}{}:
		defer func() { <-s.inFlight }()
	case <-r.Context().Done():
		status = http.StatusRequestTimeout
		writeJSONError(w, status, "request_cancelled")
		return
	default:
		status = http.StatusServiceUnavailable
		w.Header().Set("Retry-After", "1")
		writeJSONError(w, status, "relay_busy")
		return
	}

	var err error
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/models":
		status, err = s.forward(w, r, http.MethodGet, s.apiBaseURL+"/models", nil)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions":
		var body []byte
		body, err = readRequestBody(r.Body, s.maxRequestBytes)
		if err != nil {
			if errors.Is(err, errRequestTooLarge) {
				status = http.StatusRequestEntityTooLarge
				writeJSONError(w, status, "request_too_large")
			} else {
				status = http.StatusBadRequest
				writeJSONError(w, status, "invalid_request_body")
			}
			return
		}
		body, err = normalizeKimiChatRequest(body)
		if err != nil {
			status = http.StatusBadRequest
			writeJSONError(w, status, "invalid_request")
			return
		}
		status, err = s.forward(w, r, http.MethodPost, s.apiBaseURL+"/chat/completions", body)
	default:
		status = http.StatusNotFound
		writeJSONError(w, status, "not_found")
		return
	}
	if err != nil {
		if !tracked.Written() {
			status = http.StatusBadGateway
			writeJSONError(w, status, "upstream_unavailable")
		}
		slog.Warn("kimi relay upstream failure", "method", r.Method, "path", r.URL.Path, "error", err)
	}
}

func (s *relayServer) authorized(header string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	candidate := []byte(strings.TrimSpace(strings.TrimPrefix(header, prefix)))
	return len(candidate) == len(s.secret) && subtle.ConstantTimeCompare(candidate, s.secret) == 1
}

func (s *relayServer) forward(w http.ResponseWriter, inbound *http.Request, method, target string, body []byte) (int, error) {
	token, err := s.tokens.AccessToken(inbound.Context(), false)
	if err != nil {
		return http.StatusBadGateway, err
	}
	resp, err := s.doUpstream(inbound.Context(), inbound.Header, method, target, body, token)
	if err != nil {
		return http.StatusBadGateway, err
	}
	if resp.StatusCode == http.StatusUnauthorized {
		_ = resp.Body.Close()
		token, err = s.tokens.AccessToken(inbound.Context(), true)
		if err != nil {
			return http.StatusBadGateway, err
		}
		resp, err = s.doUpstream(inbound.Context(), inbound.Header, method, target, body, token)
		if err != nil {
			return http.StatusBadGateway, err
		}
	}
	defer func() { _ = resp.Body.Close() }()

	copyResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if err := copyStreamingBody(w, resp.Body); err != nil {
		return resp.StatusCode, err
	}
	return resp.StatusCode, nil
}

func (s *relayServer) doUpstream(ctx context.Context, inbound http.Header, method, target string, body []byte, token string) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return nil, err
	}
	copyHeaders(req.Header, s.identityHeaders)
	req.Header.Set("Authorization", "Bearer "+token)
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	if accept := strings.TrimSpace(inbound.Get("Accept")); accept != "" {
		req.Header.Set("Accept", accept)
	} else {
		req.Header.Set("Accept", "application/json")
	}
	if language := strings.TrimSpace(inbound.Get("Accept-Language")); language != "" {
		req.Header.Set("Accept-Language", language)
	}
	return s.client.Do(req)
}

func normalizeKimiChatRequest(body []byte) ([]byte, error) {
	var request map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&request); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, errors.New("request must be a JSON object")
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("request contains trailing JSON")
	}
	request["model"] = json.RawMessage(`"k3"`)

	effort, found, err := extractKimiEffort(request)
	if err != nil {
		return nil, err
	}
	delete(request, "reasoning_effort")
	delete(request, "reasoning")
	if found {
		thinking := make(map[string]json.RawMessage)
		if raw := request["thinking"]; len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			if err := json.Unmarshal(raw, &thinking); err != nil {
				return nil, errors.New("thinking must be an object")
			}
		}
		thinking["type"] = json.RawMessage(`"enabled"`)
		encodedEffort, _ := json.Marshal(effort)
		thinking["effort"] = encodedEffort
		encodedThinking, err := json.Marshal(thinking)
		if err != nil {
			return nil, err
		}
		request["thinking"] = encodedThinking
	}
	return json.Marshal(request)
}

func extractKimiEffort(request map[string]json.RawMessage) (string, bool, error) {
	if raw, ok := request["reasoning_effort"]; ok {
		return parseKimiEffort(raw)
	}
	if raw, ok := request["reasoning"]; ok {
		var reasoning map[string]json.RawMessage
		if err := json.Unmarshal(raw, &reasoning); err != nil {
			return "", false, errors.New("reasoning must be an object")
		}
		if effort, ok := reasoning["effort"]; ok {
			return parseKimiEffort(effort)
		}
	}
	if raw, ok := request["thinking"]; ok {
		var thinking map[string]json.RawMessage
		if err := json.Unmarshal(raw, &thinking); err != nil {
			return "", false, errors.New("thinking must be an object")
		}
		if effort, ok := thinking["effort"]; ok {
			return parseKimiEffort(effort)
		}
	}
	return "", false, nil
}

func parseKimiEffort(raw json.RawMessage) (string, bool, error) {
	var effort string
	if err := json.Unmarshal(raw, &effort); err != nil {
		return "", false, errors.New("reasoning effort must be a string")
	}
	effort = strings.ToLower(strings.TrimSpace(effort))
	switch effort {
	case "low", "high", "max":
		return effort, true, nil
	default:
		return "", false, fmt.Errorf("unsupported KIMI effort %q", effort)
	}
}

var errRequestTooLarge = errors.New("request body is too large")

func readRequestBody(body io.ReadCloser, limit int64) ([]byte, error) {
	defer func() { _ = body.Close() }()
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errRequestTooLarge
	}
	return data, nil
}

func copyResponseHeaders(dst, src http.Header) {
	for _, key := range []string{
		"Content-Type", "Cache-Control", "Retry-After", "X-Request-Id", "X-Trace-Id",
		"Moonshot-Request-Id", "OpenAI-Processing-Ms",
	} {
		for _, value := range src.Values(key) {
			dst.Add(key, value)
		}
	}
}

func copyStreamingBody(w http.ResponseWriter, body io.Reader) error {
	buffer := make([]byte, 32<<10)
	flusher, _ := w.(http.Flusher)
	for {
		n, readErr := body.Read(buffer)
		if n > 0 {
			if _, err := w.Write(buffer[:n]); err != nil {
				return err
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		}
	}
}

func writeJSONError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{
			"type":    code,
			"message": http.StatusText(status),
		},
	})
}

type trackingResponseWriter struct {
	http.ResponseWriter
	written bool
}

func (w *trackingResponseWriter) WriteHeader(status int) {
	if w.written {
		return
	}
	w.written = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *trackingResponseWriter) Write(data []byte) (int, error) {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

func (w *trackingResponseWriter) Flush() {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *trackingResponseWriter) Written() bool {
	return w.written
}

func parseProxyURL(raw string) (*url.URL, error) {
	return url.Parse(raw)
}
