package service

import (
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
)

// streamDataIntervalTimeoutForRequest keeps the ordinary hung-upstream guard
// while allowing Claude Code to wait for long-running agent/tool work. The
// downstream keepalive remains independent and continues to protect proxies.
func streamDataIntervalTimeoutForRequest(cfg *config.Config, c *gin.Context) time.Duration {
	if cfg == nil {
		return 0
	}

	seconds := cfg.Gateway.StreamDataIntervalTimeout
	if isClaudeCodeStreamRequest(c) && cfg.Gateway.ClaudeCodeStreamDataIntervalTimeout > 0 {
		seconds = cfg.Gateway.ClaudeCodeStreamDataIntervalTimeout
	}
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

// claudeCodeStreamDataIntervalTimeoutForRequest deliberately returns zero for
// non-Claude requests. The raw Chat Completions compatibility bridge historically
// had no interval guard; keep that behavior for other clients while applying the
// Claude Code long-tool-wait policy consistently across both bridge paths.
func claudeCodeStreamDataIntervalTimeoutForRequest(cfg *config.Config, c *gin.Context) time.Duration {
	if !isClaudeCodeStreamRequest(c) {
		return 0
	}
	return streamDataIntervalTimeoutForRequest(cfg, c)
}

func isClaudeCodeStreamRequest(c *gin.Context) bool {
	if c == nil || c.Request == nil {
		return false
	}
	// The handler normally annotates the request context after validating the
	// Claude Code body. Keep the UA fallback for compatibility handlers that do
	// not run that annotation before entering the stream converter.
	if IsClaudeCodeClient(c.Request.Context()) {
		return true
	}
	return claudeCliUserAgentRe.MatchString(strings.TrimSpace(c.GetHeader("User-Agent")))
}
