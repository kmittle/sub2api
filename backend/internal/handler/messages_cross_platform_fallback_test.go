package handler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func newMessagesFallbackTestContext(t *testing.T, body []byte, group *service.Group) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	writer := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(writer)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 9, Group: group})
	return c, writer
}

func messagesFallbackTestGroup() *service.Group {
	return &service.Group{
		ID:       3,
		Platform: service.PlatformComposite,
		MessagesDispatchModelConfig: service.OpenAIMessagesDispatchModelConfig{
			OpusMappedModel: "gpt-5.6-sol",
			Fallback: &service.OpenAIMessagesDispatchFallbackConfig{
				Enabled:        true,
				TargetPlatform: service.PlatformAnthropic,
			},
		},
	}
}

func TestTryMessagesCrossPlatformFallback_TransfersOnlyBeforeResponseCommit(t *testing.T) {
	const body = `{"model":"claude-opus-5","messages":[]}`
	group := messagesFallbackTestGroup()

	t.Run("transfers and restores headers", func(t *testing.T) {
		c, _ := newMessagesFallbackTestContext(t, []byte(body), group)
		c.Writer.Header().Set("X-Gateway", "preserve")
		baseline := c.Writer.Header().Clone()
		c.Writer.Header().Set("Content-Type", "text/event-stream")
		c.Writer.Header().Set("X-Request-Id", "openai-attempt")

		var released int
		var called bool
		h := &OpenAIGatewayHandler{}
		h.SetMessagesFallbackHandler(func(got *gin.Context, gotBody []byte, model, platform string) {
			called = true
			require.Equal(t, body, string(gotBody))
			require.Equal(t, "claude-opus-5", model)
			require.Equal(t, service.PlatformAnthropic, platform)
			require.Equal(t, 1, released)
		})

		ok := h.tryMessagesCrossPlatformFallback(c, &service.APIKey{Group: group}, []byte(body), "claude-opus-5", baseline,
			func() { released++ }, nil, service.ErrNoAvailableAccounts)
		require.True(t, ok)
		require.True(t, called)
		require.Equal(t, baseline, c.Writer.Header())
	})

	t.Run("does not transfer after body write", func(t *testing.T) {
		c, _ := newMessagesFallbackTestContext(t, []byte(body), group)
		_, err := c.Writer.Write([]byte("partial"))
		require.NoError(t, err)
		called := false
		h := &OpenAIGatewayHandler{}
		h.SetMessagesFallbackHandler(func(*gin.Context, []byte, string, string) { called = true })

		require.False(t, h.tryMessagesCrossPlatformFallback(c, &service.APIKey{Group: group}, []byte(body), "claude-opus-5", nil, nil, nil, nil))
		require.False(t, called)
	})

	t.Run("does not transfer after header commit", func(t *testing.T) {
		c, _ := newMessagesFallbackTestContext(t, []byte(body), group)
		c.Writer.WriteHeaderNow()
		called := false
		h := &OpenAIGatewayHandler{}
		h.SetMessagesFallbackHandler(func(*gin.Context, []byte, string, string) { called = true })

		require.False(t, h.tryMessagesCrossPlatformFallback(c, &service.APIKey{Group: group}, []byte(body), "claude-opus-5", nil, nil, nil, nil))
		require.False(t, called)
	})

	t.Run("does not transfer an unsupported family", func(t *testing.T) {
		c, _ := newMessagesFallbackTestContext(t, []byte(`{"model":"claude-fable-5","messages":[]}`), group)
		called := false
		h := &OpenAIGatewayHandler{}
		h.SetMessagesFallbackHandler(func(*gin.Context, []byte, string, string) { called = true })

		require.False(t, h.tryMessagesCrossPlatformFallback(c, &service.APIKey{Group: group}, []byte(`{"model":"claude-fable-5","messages":[]}`), "claude-fable-5", nil, nil, nil, nil))
		require.False(t, called)
	})
}

func TestTryMessagesCrossPlatformFallback_ReleaseCanBeCalledExactlyOnce(t *testing.T) {
	group := messagesFallbackTestGroup()
	c, _ := newMessagesFallbackTestContext(t, []byte(`{"model":"claude-opus-5"}`), group)
	var releaseCount int
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { releaseCount++ })
	}
	called := 0
	h := &OpenAIGatewayHandler{}
	h.SetMessagesFallbackHandler(func(*gin.Context, []byte, string, string) { called++ })

	require.True(t, h.tryMessagesCrossPlatformFallback(c, &service.APIKey{Group: group}, []byte(`{"model":"claude-opus-5"}`), "claude-opus-5", nil, release, nil, service.ErrNoAvailableAccounts))
	require.Equal(t, 1, releaseCount)
	require.Equal(t, 1, called)

	// The outer Messages handler owns the once-wrapped release function and
	// invokes it again from defer after the callback returns.
	release()
	require.Equal(t, 1, releaseCount)
}

func TestMessagesCrossPlatformFallback_RejectsNonEligibleCause(t *testing.T) {
	group := messagesFallbackTestGroup()
	causes := []error{
		errors.New("database unavailable"),
		fmt.Errorf("%w supporting model: gpt-test (channel pricing restriction)", service.ErrNoAvailableAccounts),
		&service.UpstreamFailoverError{NextAccountAction: service.NextAccountStop},
	}
	for _, cause := range causes {
		c, _ := newMessagesFallbackTestContext(t, []byte(`{"model":"claude-opus-5"}`), group)
		called := false
		h := &OpenAIGatewayHandler{}
		h.SetMessagesFallbackHandler(func(*gin.Context, []byte, string, string) { called = true })
		require.False(t, h.tryMessagesCrossPlatformFallback(c, &service.APIKey{Group: group}, []byte(`{"model":"claude-opus-5"}`), "claude-opus-5", nil, nil, nil, cause))
		require.False(t, called)
	}
}

func TestMessagesCrossPlatformFallback_CurrentSelectionPolicyErrorWinsAfterUpstreamFailure(t *testing.T) {
	const body = `{"model":"claude-opus-5"}`
	group := messagesFallbackTestGroup()
	previousUpstreamFailure := &service.UpstreamFailoverError{
		StatusCode:        http.StatusServiceUnavailable,
		NextAccountAction: service.NextAccountRetry,
	}
	currentSelectionErr := fmt.Errorf("%w supporting model: gpt-test (channel pricing restriction)", service.ErrNoAvailableAccounts)
	c, writer := newMessagesFallbackTestContext(t, []byte(body), group)
	called := false
	h := &OpenAIGatewayHandler{}
	h.SetMessagesFallbackHandler(func(*gin.Context, []byte, string, string) { called = true })

	require.True(t, messagesCrossPlatformFallbackCauseEligible(previousUpstreamFailure),
		"the previous upstream error would have allowed fallback")
	h.handleMessagesAccountSelectionFailureAfterFailover(
		c,
		&service.APIKey{Group: group},
		[]byte(body),
		"claude-opus-5",
		nil,
		nil,
		nil,
		currentSelectionErr,
		previousUpstreamFailure,
		false,
	)

	require.False(t, called, "the current channel pricing restriction must prevent fallback")
	require.Equal(t, http.StatusServiceUnavailable, writer.Code)
}

func TestGatewayMessagesCrossPlatformFallback_ResetsCompositeRoutingContext(t *testing.T) {
	const body = `{"model":"claude-opus-5","messages":[]}`
	group := messagesFallbackTestGroup()
	c, writer := newMessagesFallbackTestContext(t, []byte(body), group)
	ctx := service.WithResolvedTargetPlatform(c.Request.Context(), service.PlatformOpenAI)
	ctx = context.WithValue(ctx, ctxkey.ResolvedUpstreamModel, "gpt-5.6-sol")
	ctx = context.WithValue(ctx, ctxkey.RequestedPublicModel, "claude-opus-5")
	ctx = context.WithValue(ctx, ctxkey.AccountID, int64(1))
	ctx = context.WithValue(ctx, ctxkey.Platform, service.PlatformOpenAI)
	c.Request = c.Request.WithContext(ctx)
	c.Set(opsAccountIDKey, int64(1))

	// The native handler will stop at the missing auth subject. That is enough
	// to verify the transfer preparation without constructing all gateway deps.
	(&GatewayHandler{}).MessagesCrossPlatformFallback(c, []byte(body), "claude-opus-5", service.PlatformAnthropic)

	resolved, ok := service.ResolvedTargetPlatformFromContext(c.Request.Context())
	require.True(t, ok)
	require.Equal(t, service.PlatformAnthropic, resolved)
	upstream, ok := service.ResolvedUpstreamModelFromContext(c.Request.Context())
	require.True(t, ok)
	require.Equal(t, "claude-opus-5", upstream)
	publicModel, ok := service.RequestedPublicModelFromContext(c.Request.Context())
	require.True(t, ok)
	require.Equal(t, "claude-opus-5", publicModel)
	require.Equal(t, service.PlatformAnthropic, c.Request.Context().Value(ctxkey.ForcePlatform))
	require.Equal(t, int64(0), c.Request.Context().Value(ctxkey.AccountID))
	require.Equal(t, service.PlatformAnthropic, c.Request.Context().Value(ctxkey.Platform))
	forcePlatform, ok := middleware2.GetForcePlatformFromContext(c)
	require.True(t, ok)
	require.Equal(t, service.PlatformAnthropic, forcePlatform)
	selected, ok := c.Get(opsAccountIDKey)
	require.True(t, ok)
	require.Equal(t, int64(0), selected)
	require.NotEmpty(t, writer.Body.String())
}

func TestProvideHandlersInjectsMessagesFallbackHandler(t *testing.T) {
	openai := &OpenAIGatewayHandler{}
	gateway := &GatewayHandler{}
	handlers := ProvideHandlers(
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		gateway, openai,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	require.Same(t, gateway, handlers.Gateway)
	require.Same(t, openai, handlers.OpenAIGateway)
	require.NotNil(t, openai.messagesFallbackHandler)
}
