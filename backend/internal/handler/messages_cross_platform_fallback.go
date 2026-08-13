package handler

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

const messagesCrossPlatformFallbackGinKey = "messages_cross_platform_fallback"

type messagesCrossPlatformFallbackContextKey struct{}

// MessagesFallbackHandler transfers an untouched Anthropic Messages request
// from the OpenAI bridge to a concrete fallback platform. The handler owns the
// response once invoked.
type MessagesFallbackHandler func(c *gin.Context, body []byte, requestedModel, targetPlatform string)

func (h *OpenAIGatewayHandler) SetMessagesFallbackHandler(handler MessagesFallbackHandler) {
	if h != nil {
		h.messagesFallbackHandler = handler
	}
}

func (h *OpenAIGatewayHandler) tryMessagesCrossPlatformFallback(
	c *gin.Context,
	apiKey *service.APIKey,
	body []byte,
	requestedModel string,
	responseHeadersBeforeDispatch http.Header,
	releaseUserSlot func(),
	reqLog *zap.Logger,
	cause error,
) bool {
	if h == nil || h.messagesFallbackHandler == nil || c == nil || c.Request == nil || c.Writer == nil {
		return false
	}
	// Once headers or a body are committed, the fallback cannot reliably change
	// the status code or response shape. Never splice two upstream attempts into
	// one client-visible response.
	if c.Writer.Written() || c.Writer.Size() > 0 || service.IsResponseCommitted(c) {
		return false
	}
	if apiKey == nil || apiKey.Group == nil {
		return false
	}
	// Selection failures are not all account failures. In particular, a
	// channel/pricing policy veto or an infrastructure/configuration error must
	// not be bypassed by entering Anthropic. Upstream failures are accepted only
	// when the normal failover policy explicitly says another account is valid.
	if !messagesCrossPlatformFallbackCauseEligible(cause) {
		return false
	}
	targetPlatform, ok := apiKey.Group.ResolveMessagesDispatchFallbackPlatform(requestedModel)
	if !ok || targetPlatform != service.PlatformAnthropic {
		return false
	}
	if c.Request.Context().Err() != nil {
		return false
	}
	modelResult := gjson.GetBytes(body, "model")
	if !modelResult.Exists() || modelResult.Type != gjson.String ||
		strings.TrimSpace(modelResult.String()) != strings.TrimSpace(requestedModel) {
		return false
	}

	// OpenAI response handling may stage provider headers before it knows an
	// attempt must fail over. Restore the pre-dispatch header set so those
	// headers cannot leak into the Anthropic response.
	restoreMessagesFallbackResponseHeaders(c.Writer.Header(), responseHeadersBeforeDispatch)

	if releaseUserSlot != nil {
		releaseUserSlot()
	}
	if reqLog != nil {
		fields := []zap.Field{
			zap.String("requested_model", requestedModel),
			zap.String("target_platform", targetPlatform),
		}
		if cause != nil {
			fields = append(fields, zap.Error(cause))
		}
		reqLog.Warn("openai_messages.cross_platform_fallback", fields...)
	}
	h.messagesFallbackHandler(c, append([]byte(nil), body...), requestedModel, targetPlatform)
	return true
}

func (h *OpenAIGatewayHandler) handleMessagesAccountSelectionFailureAfterFailover(
	c *gin.Context,
	apiKey *service.APIKey,
	body []byte,
	requestedModel string,
	responseHeadersBeforeDispatch http.Header,
	releaseUserSlot func(),
	reqLog *zap.Logger,
	selectionErr error,
	lastFailoverErr *service.UpstreamFailoverError,
	streamStarted bool,
) {
	// The current selection error may represent a newly applied policy veto.
	// Never let the previous upstream failure determine fallback eligibility.
	if h.tryMessagesCrossPlatformFallback(
		c,
		apiKey,
		body,
		requestedModel,
		responseHeadersBeforeDispatch,
		releaseUserSlot,
		reqLog,
		selectionErr,
	) {
		return
	}
	if lastFailoverErr != nil {
		h.handleAnthropicFailoverExhausted(c, lastFailoverErr, streamStarted)
		return
	}
	h.anthropicStreamingAwareError(c, http.StatusBadGateway, "api_error", "Upstream request failed", streamStarted)
}

func messagesCrossPlatformFallbackCauseEligible(cause error) bool {
	if cause == nil {
		return false
	}

	var failoverErr *service.UpstreamFailoverError
	if errors.As(cause, &failoverErr) {
		return failoverErr.ShouldRetryNextAccount()
	}

	// The scheduler wraps ordinary account-pool exhaustion with this sentinel.
	// ErrNoAvailableCompactAccounts is intentionally excluded: it represents an
	// endpoint-capability mismatch, not a generic account outage for Messages.
	if !errors.Is(cause, service.ErrNoAvailableAccounts) {
		return false
	}

	// Channel pricing restrictions are policy/billing decisions, not temporary
	// account unavailability. Do not let fallback silently bypass them.
	return !strings.Contains(strings.ToLower(cause.Error()), "channel pricing restriction")
}

func restoreMessagesFallbackResponseHeaders(dst, snapshot http.Header) {
	for key := range dst {
		delete(dst, key)
	}
	for key, values := range snapshot {
		dst[key] = append([]string(nil), values...)
	}
}

// MessagesCrossPlatformFallback re-enters the native Anthropic Messages
// pipeline while preserving the original request model. Account-level
// model_mapping remains the sole authority for any alias and capability
// decision on the fallback platform.
func (h *GatewayHandler) MessagesCrossPlatformFallback(c *gin.Context, body []byte, requestedModel, targetPlatform string) {
	if h == nil || c == nil || c.Request == nil || c.Writer == nil {
		return
	}
	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok || apiKey == nil || apiKey.Group == nil || apiKey.Group.Platform != service.PlatformComposite {
		h.errorResponse(c, http.StatusInternalServerError, "api_error", "Cross-platform fallback is not available")
		return
	}
	configuredTarget, enabled := apiKey.Group.ResolveMessagesDispatchFallbackPlatform(requestedModel)
	if !enabled || configuredTarget != targetPlatform || targetPlatform != service.PlatformAnthropic {
		h.errorResponse(c, http.StatusInternalServerError, "api_error", "Cross-platform fallback configuration is invalid")
		return
	}
	modelResult := gjson.GetBytes(body, "model")
	if !modelResult.Exists() || modelResult.Type != gjson.String ||
		strings.TrimSpace(modelResult.String()) != strings.TrimSpace(requestedModel) {
		h.errorResponse(c, http.StatusInternalServerError, "api_error", "Cross-platform fallback request is invalid")
		return
	}

	ctx := service.WithResolvedTargetPlatform(c.Request.Context(), targetPlatform)
	ctx = context.WithValue(ctx, ctxkey.ForcePlatform, targetPlatform)
	ctx = context.WithValue(ctx, ctxkey.ResolvedUpstreamModel, strings.TrimSpace(requestedModel))
	ctx = context.WithValue(ctx, ctxkey.RequestedPublicModel, strings.TrimSpace(requestedModel))
	ctx = context.WithValue(ctx, ctxkey.AccountID, int64(0))
	ctx = context.WithValue(ctx, ctxkey.Platform, targetPlatform)
	ctx = context.WithValue(ctx, messagesCrossPlatformFallbackContextKey{}, true)
	c.Request = c.Request.WithContext(ctx)
	c.Set(string(middleware2.ContextKeyForcePlatform), targetPlatform)
	c.Set(messagesCrossPlatformFallbackGinKey, true)
	c.Set(opsAccountIDKey, int64(0))

	requestBody := append([]byte(nil), body...)
	c.Request.Body = io.NopCloser(bytes.NewReader(requestBody))
	c.Request.ContentLength = int64(len(requestBody))
	c.Request.Header.Set("Content-Length", strconv.Itoa(len(requestBody)))
	for _, header := range []string{"Retry-After", "X-Request-Id", "OpenAI-Request-Id"} {
		c.Writer.Header().Del(header)
	}

	requestLogger(c, "handler.gateway.messages_fallback",
		zap.String("requested_model", strings.TrimSpace(requestedModel)),
		zap.String("target_platform", targetPlatform),
	).Info("gateway.messages_cross_platform_fallback_started")
	h.Messages(c)
}
