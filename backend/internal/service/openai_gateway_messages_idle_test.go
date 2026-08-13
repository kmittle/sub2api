//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// This is the shape of a Claude Code turn that pauses while an agent or tool
// runs: the upstream stream is open, but no new SSE line arrives for a while.
func TestHandleAnthropicStreamingResponse_ClaudeCodeIdleGapReproducesTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"stream":true}`))
	c.Request = c.Request.WithContext(context.Background())
	c.Request.Header.Set("User-Agent", "claude-cli/2.1.220 (external, cli)")

	reader, writer := io.Pipe()
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       reader,
	}
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{
		StreamDataIntervalTimeout: 1,
		StreamKeepaliveInterval:   1,
		MaxLineSize:               defaultMaxLineSize,
	}}}
	account := &Account{ID: 101, Platform: PlatformOpenAI, Type: AccountTypeOAuth}

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_idle\",\"model\":\"gpt-test\",\"status\":\"in_progress\",\"output\":[]}}\n\n")
		time.Sleep(1500 * time.Millisecond)
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_idle\",\"object\":\"response\",\"model\":\"gpt-test\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"id\":\"msg_idle\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"ok\"}]}],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n")
		_ = writer.Close()
	}()

	_, err := svc.handleAnthropicStreamingResponse(resp, c, account, "claude-test", "gpt-test", "gpt-test", time.Now())
	_ = reader.Close()
	<-writerDone

	require.Error(t, err)
	require.Contains(t, err.Error(), "stream data interval timeout")
}

func TestHandleAnthropicStreamingResponse_ClaudeCodeIdleGapUsesExtendedTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"stream":true}`))
	c.Request = c.Request.WithContext(context.Background())
	c.Request.Header.Set("User-Agent", "claude-cli/2.1.220 (external, cli)")

	reader, writer := io.Pipe()
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       reader,
	}
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{
		StreamDataIntervalTimeout:           1,
		ClaudeCodeStreamDataIntervalTimeout: 60,
		StreamKeepaliveInterval:             1,
		MaxLineSize:                         defaultMaxLineSize,
	}}}
	account := &Account{ID: 102, Platform: PlatformOpenAI, Type: AccountTypeOAuth}

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_idle\",\"model\":\"gpt-test\",\"status\":\"in_progress\",\"output\":[]}}\n\n")
		time.Sleep(1500 * time.Millisecond)
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_idle\",\"object\":\"response\",\"model\":\"gpt-test\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"id\":\"msg_idle\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"ok\"}]}],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n")
		_ = writer.Close()
	}()

	result, err := svc.handleAnthropicStreamingResponse(resp, c, account, "claude-test", "gpt-test", "gpt-test", time.Now())
	_ = reader.Close()
	<-writerDone

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Contains(t, recorder.Body.String(), "message_stop")
}
