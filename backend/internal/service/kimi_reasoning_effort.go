package service

import (
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Kimi K3 exposes three ordered reasoning levels: low, high and max.
// Claude Code and Codex clients expose a larger, partly overlapping set of
// names (minimal/low/medium/high/xhigh/max). Kimi mapping is therefore based
// on ordinal position, never on the literal source name: the strongest source
// level is always Kimi max, the next source level Kimi high, and the weakest
// source level Kimi low.
var kimiReasoningEffortLevels = [...]string{"low", "high", "max"}

var (
	// The order is the public order exposed by the two client protocols.
	// Mapping uses these ranks, never same-name matching.
	claudeReasoningEffortOrder = []string{"low", "medium", "high", "max"}
	openAIReasoningEffortOrder = []string{"minimal", "low", "medium", "high", "xhigh", "max"}
)

func isKimiModel(model string) bool {
	id := strings.ToLower(strings.TrimSpace(model))
	if slash := strings.LastIndexByte(id, '/'); slash >= 0 {
		id = strings.TrimSpace(id[slash+1:])
	}
	return strings.HasPrefix(id, "kimi-") || strings.HasPrefix(id, "moonshot-") || id == "k3" || id == "k3-256k"
}

func normalizeEffortLabel(raw string) string {
	value := strings.ToLower(strings.TrimSpace(raw))
	return strings.NewReplacer("-", "", "_", "", " ", "").Replace(value)
}

// mapKimiEffortByOrdinal compresses an ordered source vocabulary into Kimi's
// ordered vocabulary from the top down. Thus the strongest source level maps
// to Kimi max and the second strongest source level maps to Kimi high; any
// additional weaker source levels collapse to Kimi low.
func mapKimiEffortByOrdinal(raw string, sourceOrder []string) string {
	value := normalizeEffortLabel(raw)
	if value == "" || len(sourceOrder) == 0 {
		return ""
	}
	rank := -1
	for i, source := range sourceOrder {
		if normalizeEffortLabel(source) == value {
			rank = i
			break
		}
	}
	if rank < 0 {
		return ""
	}
	fromTop := len(sourceOrder) - 1 - rank
	targetFromTop := fromTop
	if targetFromTop >= len(kimiReasoningEffortLevels) {
		targetFromTop = len(kimiReasoningEffortLevels) - 1
	}
	return kimiReasoningEffortLevels[len(kimiReasoningEffortLevels)-1-targetFromTop]
}

func mapKimiAnthropicEffort(raw string) string {
	return mapKimiEffortByOrdinal(raw, claudeReasoningEffortOrder)
}

func mapKimiOpenAIReasoningEffort(raw string) string {
	return mapKimiEffortByOrdinal(raw, openAIReasoningEffortOrder)
}

func mapKimiOpenAIReasoningEffortForModel(raw, sourceModel string) string {
	// A request that already names a Kimi model is speaking Kimi's native
	// vocabulary. Preserve the three supported values instead of interpreting
	// `high` through the larger OpenAI vocabulary and accidentally collapsing
	// it to `low`. Unknown OpenAI aliases still use ordinal mapping below.
	if isKimiModel(sourceModel) {
		switch normalizeEffortLabel(raw) {
		case "low":
			return "low"
		case "high":
			return "high"
		case "max":
			return "max"
		case "minimal", "medium":
			return "low"
		case "xhigh", "extrahigh":
			return "high"
		default:
			return ""
		}
	}

	order := openAIReasoningEffortOrder
	// GPT-5.6 models expose the four practical CLI tiers low/medium/high/max.
	// Use that model's actual ordered scale so high remains the second-highest
	// tier; older/future OpenAI models keep the full compatibility scale.
	if isOpenAIGPT56Model(sourceModel) {
		order = []string{"low", "medium", "high", "max"}
	}
	return mapKimiEffortByOrdinal(raw, order)
}

// mapKimiReasoningEffort is retained for callers that do not have a protocol
// marker; OpenAI is the superset vocabulary and therefore the conservative
// default.
func mapKimiReasoningEffort(raw string) string {
	return mapKimiOpenAIReasoningEffort(raw)
}

func normalizeKimiReasoningEffortBody(body []byte, model string) ([]byte, bool) {
	return normalizeKimiOpenAIReasoningEffortBody(body, model, "")
}

func normalizeKimiOpenAIReasoningEffortBody(body []byte, model, sourceModel string) ([]byte, bool) {
	if !isKimiModel(model) || len(body) == 0 {
		return body, false
	}
	result := body
	changed := false
	for _, path := range []string{"reasoning.effort", "reasoning_effort"} {
		field := gjson.GetBytes(result, path)
		if !field.Exists() || field.Type != gjson.String {
			continue
		}
		mapped := mapKimiOpenAIReasoningEffortForModel(field.String(), sourceModel)
		if mapped == "" || mapped == field.String() {
			continue
		}
		updated, err := sjson.SetBytes(result, path, mapped)
		if err != nil {
			continue
		}
		result = updated
		changed = true
	}
	return result, changed
}

// extractKimiNativeReasoningEffortFromBody reads effort after the request body
// has been normalized for Kimi. It deliberately does not run the generic
// OpenAI normalizer: Kimi's native `max` must remain `max` for billing and
// usage metadata rather than being recorded as OpenAI's legacy `xhigh`.
func extractKimiNativeReasoningEffortFromBody(body []byte) *string {
	for _, path := range []string{"reasoning.effort", "reasoning_effort"} {
		field := gjson.GetBytes(body, path)
		if !field.Exists() || field.Type != gjson.String {
			continue
		}
		switch value := normalizeEffortLabel(field.String()); value {
		case "low", "high", "max":
			native := value
			return &native
		}
	}
	return nil
}

// normalizeKimiAnthropicEffort maps output_config.effort before the Anthropic
// bridge converts it to Responses or Chat Completions. The returned value is
// a Kimi-native level and is also suitable for usage metadata.
func normalizeKimiAnthropicEffort(raw, model string) string {
	if !isKimiModel(model) {
		return raw
	}
	if mapped := mapKimiAnthropicEffort(raw); mapped != "" {
		return mapped
	}
	return raw
}
