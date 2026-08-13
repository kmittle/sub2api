package service

import (
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
)

const (
	defaultOpenAIMessagesDispatchOpusMappedModel   = "gpt-5.4"
	defaultOpenAIMessagesDispatchSonnetMappedModel = "gpt-5.3-codex"
	defaultOpenAIMessagesDispatchHaikuMappedModel  = "gpt-5.4-mini"
)

func normalizeOpenAIMessagesDispatchMappedModel(model string) string {
	model = NormalizeOpenAICompatRequestedModel(strings.TrimSpace(model))
	return strings.TrimSpace(model)
}

func normalizeOpenAIMessagesDispatchModelConfig(cfg OpenAIMessagesDispatchModelConfig) OpenAIMessagesDispatchModelConfig {
	out := OpenAIMessagesDispatchModelConfig{
		OpusMappedModel:   normalizeOpenAIMessagesDispatchMappedModel(cfg.OpusMappedModel),
		SonnetMappedModel: normalizeOpenAIMessagesDispatchMappedModel(cfg.SonnetMappedModel),
		HaikuMappedModel:  normalizeOpenAIMessagesDispatchMappedModel(cfg.HaikuMappedModel),
	}

	if len(cfg.ExactModelMappings) > 0 {
		out.ExactModelMappings = make(map[string]string, len(cfg.ExactModelMappings))
		for requestedModel, mappedModel := range cfg.ExactModelMappings {
			requestedModel = strings.TrimSpace(requestedModel)
			mappedModel = normalizeOpenAIMessagesDispatchMappedModel(mappedModel)
			if requestedModel == "" || mappedModel == "" {
				continue
			}
			out.ExactModelMappings[requestedModel] = mappedModel
		}
		if len(out.ExactModelMappings) == 0 {
			out.ExactModelMappings = nil
		}
	}

	if cfg.Fallback != nil {
		targetPlatform := strings.ToLower(strings.TrimSpace(cfg.Fallback.TargetPlatform))
		// Cross-platform Messages fallback is deliberately restricted to an
		// Anthropic target for now. Other providers use different wire
		// protocols and must not be entered through the Claude Messages path.
		if targetPlatform == PlatformAnthropic && cfg.Fallback.Enabled {
			out.Fallback = &OpenAIMessagesDispatchFallbackConfig{
				Enabled:        true,
				TargetPlatform: targetPlatform,
			}
		}
	}

	return out
}

// mergeOpenAIMessagesDispatchModelConfigForUpdate keeps the server-managed
// fallback stanza when an older admin client updates only the model mappings.
// Sending an explicit fallback object with enabled=false still disables it.
func mergeOpenAIMessagesDispatchModelConfigForUpdate(current, incoming OpenAIMessagesDispatchModelConfig) OpenAIMessagesDispatchModelConfig {
	if incoming.Fallback == nil && current.Fallback != nil {
		fallbackCopy := *current.Fallback
		incoming.Fallback = &fallbackCopy
	}
	return normalizeOpenAIMessagesDispatchModelConfig(incoming)
}

func claudeMessagesDispatchFamily(model string) string {
	normalized := strings.ToLower(strings.TrimSpace(model))
	if !strings.HasPrefix(normalized, "claude") {
		return ""
	}
	switch {
	case strings.Contains(normalized, "opus"):
		return "opus"
	case strings.Contains(normalized, "sonnet"):
		return "sonnet"
	case strings.Contains(normalized, "haiku"):
		return "haiku"
	default:
		return ""
	}
}

// resolveConfiguredMessagesDispatchModel applies exact mappings first, then
// trailing-star prefix mappings. Prefix matches use the longest configured
// prefix so a specific rule cannot be shadowed by a broader one.
func resolveConfiguredMessagesDispatchModel(cfg OpenAIMessagesDispatchModelConfig, requestedModel string) (string, bool) {
	requestedModel = strings.TrimSpace(requestedModel)
	if requestedModel == "" {
		return "", false
	}
	if mappedModel := strings.TrimSpace(cfg.ExactModelMappings[requestedModel]); mappedModel != "" {
		return mappedModel, true
	}
	normalizedRequestedModel := strings.ToLower(requestedModel)
	bestPrefix := ""
	bestMappedModel := ""
	for configuredModel, mappedModel := range cfg.ExactModelMappings {
		configuredModel = strings.TrimSpace(configuredModel)
		if strings.HasSuffix(configuredModel, "*") {
			prefix := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(configuredModel, "*")))
			if prefix == "" || !strings.HasPrefix(normalizedRequestedModel, prefix) || len(prefix) <= len(bestPrefix) {
				continue
			}
			mappedModel = strings.TrimSpace(mappedModel)
			if mappedModel == "" {
				continue
			}
			bestPrefix = prefix
			bestMappedModel = mappedModel
			continue
		}
		if strings.EqualFold(configuredModel, requestedModel) && strings.TrimSpace(mappedModel) != "" {
			return strings.TrimSpace(mappedModel), true
		}
	}
	if bestMappedModel != "" {
		return bestMappedModel, true
	}
	return "", false
}

func (g *Group) ResolveMessagesDispatchModel(requestedModel string) string {
	if g == nil {
		return ""
	}
	requestedModel = strings.TrimSpace(requestedModel)
	if requestedModel == "" {
		return ""
	}

	if g.Platform == PlatformGrok {
		if claudeMessagesDispatchFamily(requestedModel) == "" {
			return ""
		}
		opts := xai.RuntimeModelMappingOptions()
		if !opts.EnableCrossClientMap {
			return ""
		}
		return xai.ModelMappingWithOptions(opts)["claude-*"]
	}

	cfg := normalizeOpenAIMessagesDispatchModelConfig(g.MessagesDispatchModelConfig)
	if mappedModel, ok := resolveConfiguredMessagesDispatchModel(cfg, requestedModel); ok {
		return mappedModel
	}

	switch claudeMessagesDispatchFamily(requestedModel) {
	case "opus":
		if mappedModel := strings.TrimSpace(cfg.OpusMappedModel); mappedModel != "" {
			return mappedModel
		}
		return defaultOpenAIMessagesDispatchOpusMappedModel
	case "sonnet":
		if mappedModel := strings.TrimSpace(cfg.SonnetMappedModel); mappedModel != "" {
			return mappedModel
		}
		return defaultOpenAIMessagesDispatchSonnetMappedModel
	case "haiku":
		if mappedModel := strings.TrimSpace(cfg.HaikuMappedModel); mappedModel != "" {
			return mappedModel
		}
		return defaultOpenAIMessagesDispatchHaikuMappedModel
	default:
		return ""
	}
}

// ResolveMessagesDispatchFallbackPlatform returns the explicitly configured
// cross-platform retry target for a Claude model. A model family must still be
// recognized by the primary Messages dispatch mapping; this prevents an
// unrelated OpenAI-compatible request from entering the fallback path.
func (g *Group) ResolveMessagesDispatchFallbackPlatform(requestedModel string) (string, bool) {
	if g == nil || g.Platform != PlatformComposite {
		return "", false
	}
	requestedModel = strings.TrimSpace(requestedModel)
	if requestedModel == "" || !strings.HasPrefix(strings.ToLower(requestedModel), "claude") {
		return "", false
	}
	cfg := normalizeOpenAIMessagesDispatchModelConfig(g.MessagesDispatchModelConfig)
	// Known Claude families use the family mapping. Custom families (for
	// example claude-fable-5) are eligible only when the group explicitly
	// declares an exact primary mapping; this keeps arbitrary Claude-looking
	// model names from entering the fallback path by accident.
	if claudeMessagesDispatchFamily(requestedModel) == "" {
		if _, mapped := resolveConfiguredMessagesDispatchModel(cfg, requestedModel); !mapped {
			return "", false
		}
	}
	if g.ResolveMessagesDispatchModel(requestedModel) == "" {
		return "", false
	}
	if cfg.Fallback == nil || !cfg.Fallback.Enabled {
		return "", false
	}
	return cfg.Fallback.TargetPlatform, true
}

func sanitizeGroupMessagesDispatchFields(g *Group) {
	if g == nil || g.Platform == PlatformOpenAI || g.Platform == PlatformComposite {
		return
	}
	g.AllowMessagesDispatch = false
	g.DefaultMappedModel = ""
	g.MessagesDispatchModelConfig = OpenAIMessagesDispatchModelConfig{}
}
