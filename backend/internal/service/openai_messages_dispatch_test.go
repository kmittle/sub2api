package service

import (
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"github.com/stretchr/testify/require"
)

func TestNormalizeOpenAIMessagesDispatchModelConfig(t *testing.T) {
	t.Parallel()

	cfg := normalizeOpenAIMessagesDispatchModelConfig(OpenAIMessagesDispatchModelConfig{
		OpusMappedModel:   " gpt-5.4-high ",
		SonnetMappedModel: "gpt-5.3-codex",
		HaikuMappedModel:  " gpt-5.4-mini-medium ",
		ExactModelMappings: map[string]string{
			" claude-sonnet-4-5-20250929 ": " gpt-5.2-high ",
			"":                             "gpt-5.4",
			"claude-opus-4-6":              " ",
		},
	})

	require.Equal(t, "gpt-5.4", cfg.OpusMappedModel)
	require.Equal(t, "gpt-5.3-codex", cfg.SonnetMappedModel)
	require.Equal(t, "gpt-5.4-mini", cfg.HaikuMappedModel)
	require.Equal(t, map[string]string{
		"claude-sonnet-4-5-20250929": "gpt-5.2",
	}, cfg.ExactModelMappings)
}

func TestResolveConfiguredMessagesDispatchModelSupportsExactAndLongestPrefix(t *testing.T) {
	t.Parallel()

	cfg := OpenAIMessagesDispatchModelConfig{
		ExactModelMappings: map[string]string{
			"claude-*":      "gpt-broad",
			"claude-opus*":  "gpt-opus",
			"claude-opus-5": "gpt-exact",
			"claude-fable*": "gpt-fable",
		},
	}

	mapped, ok := resolveConfiguredMessagesDispatchModel(cfg, "claude-opus-5")
	require.True(t, ok)
	require.Equal(t, "gpt-exact", mapped)
	mapped, ok = resolveConfiguredMessagesDispatchModel(cfg, "claude-opus-5-20260101")
	require.True(t, ok)
	require.Equal(t, "gpt-opus", mapped)
	mapped, ok = resolveConfiguredMessagesDispatchModel(cfg, "claude-fable-5")
	require.True(t, ok)
	require.Equal(t, "gpt-fable", mapped)
	mapped, ok = resolveConfiguredMessagesDispatchModel(cfg, "claude-sonnet-5")
	require.True(t, ok)
	require.Equal(t, "gpt-broad", mapped)
	_, ok = resolveConfiguredMessagesDispatchModel(cfg, "grok")
	require.False(t, ok)
}

func TestNormalizeOpenAIMessagesDispatchModelConfig_Fallback(t *testing.T) {
	t.Parallel()

	cfg := normalizeOpenAIMessagesDispatchModelConfig(OpenAIMessagesDispatchModelConfig{
		Fallback: &OpenAIMessagesDispatchFallbackConfig{
			Enabled:        true,
			TargetPlatform: " ANTHROPIC ",
		},
	})
	require.NotNil(t, cfg.Fallback)
	require.True(t, cfg.Fallback.Enabled)
	require.Equal(t, PlatformAnthropic, cfg.Fallback.TargetPlatform)

	for _, invalid := range []OpenAIMessagesDispatchFallbackConfig{
		{Enabled: false, TargetPlatform: PlatformAnthropic},
		{Enabled: true, TargetPlatform: PlatformOpenAI},
		{Enabled: true, TargetPlatform: ""},
	} {
		got := normalizeOpenAIMessagesDispatchModelConfig(OpenAIMessagesDispatchModelConfig{Fallback: &invalid})
		require.Nil(t, got.Fallback)
	}
}

func TestMergeOpenAIMessagesDispatchModelConfigForUpdate_PreservesOrDisablesFallback(t *testing.T) {
	t.Parallel()

	current := OpenAIMessagesDispatchModelConfig{
		Fallback: &OpenAIMessagesDispatchFallbackConfig{Enabled: true, TargetPlatform: PlatformAnthropic},
	}
	merged := mergeOpenAIMessagesDispatchModelConfigForUpdate(current, OpenAIMessagesDispatchModelConfig{
		OpusMappedModel: "gpt-5.6-terra",
	})
	require.NotNil(t, merged.Fallback)
	require.True(t, merged.Fallback.Enabled)
	require.Equal(t, "gpt-5.6-terra", merged.OpusMappedModel)

	disabled := mergeOpenAIMessagesDispatchModelConfigForUpdate(current, OpenAIMessagesDispatchModelConfig{
		Fallback: &OpenAIMessagesDispatchFallbackConfig{Enabled: false, TargetPlatform: PlatformAnthropic},
	})
	require.Nil(t, disabled.Fallback)
}

func TestGroupResolveMessagesDispatchFallbackPlatform(t *testing.T) {
	t.Parallel()

	group := &Group{
		Platform: PlatformComposite,
		MessagesDispatchModelConfig: OpenAIMessagesDispatchModelConfig{
			ExactModelMappings: map[string]string{
				"claude-fable-5":  "gpt-5.6-sol",
				"claude-custom-5": "gpt-5.6-sol",
			},
			Fallback: &OpenAIMessagesDispatchFallbackConfig{
				Enabled:        true,
				TargetPlatform: PlatformAnthropic,
			},
		},
	}
	target, ok := group.ResolveMessagesDispatchFallbackPlatform("claude-opus-5")
	require.True(t, ok)
	require.Equal(t, PlatformAnthropic, target)
	target, ok = group.ResolveMessagesDispatchFallbackPlatform("claude-fable-5")
	require.True(t, ok)
	require.Equal(t, PlatformAnthropic, target)
	target, ok = group.ResolveMessagesDispatchFallbackPlatform("claude-custom-5")
	require.True(t, ok)
	require.Equal(t, PlatformAnthropic, target)
	_, ok = group.ResolveMessagesDispatchFallbackPlatform("claude-fable-6")
	require.False(t, ok, "an unmapped custom Claude model must not enter fallback")
	_, ok = group.ResolveMessagesDispatchFallbackPlatform("gpt-5.6-sol")
	require.False(t, ok)
	_, ok = (&Group{Platform: PlatformOpenAI, MessagesDispatchModelConfig: group.MessagesDispatchModelConfig}).ResolveMessagesDispatchFallbackPlatform("claude-fable-5")
	require.False(t, ok)
}

func TestGroupResolveMessagesDispatchModel_GrokRequiresCrossClientMapping(t *testing.T) {
	original := xai.RuntimeModelMappingOptions()
	t.Cleanup(func() { xai.SetRuntimeModelMappingOptions(original) })
	group := &Group{Platform: PlatformGrok}

	xai.SetRuntimeModelMappingOptions(xai.ModelMappingOptions{})
	require.Empty(t, group.ResolveMessagesDispatchModel("claude-sonnet-4-5"))

	xai.SetRuntimeModelMappingOptions(xai.ModelMappingOptions{
		DefaultText:          "grok-build-0.1",
		EnableCrossClientMap: true,
	})
	require.Equal(t, "grok-build-0.1", group.ResolveMessagesDispatchModel("claude-sonnet-4-5"))
	require.Equal(t, "grok-build-0.1", group.ResolveMessagesDispatchModel("claude-opus-4-6"))
	require.Equal(t, "grok-build-0.1", group.ResolveMessagesDispatchModel("claude-haiku-4-5"))
	require.Empty(t, group.ResolveMessagesDispatchModel("grok"))
	require.Empty(t, group.ResolveMessagesDispatchModel("gpt-5.3-codex"))
}

func TestGroupResolveMessagesDispatchModel_UsesClaudePrefixMappings(t *testing.T) {
	t.Parallel()

	group := &Group{
		Platform: PlatformComposite,
		MessagesDispatchModelConfig: OpenAIMessagesDispatchModelConfig{
			ExactModelMappings: map[string]string{
				"claude-fable*":  "gpt-5.6-sol",
				"claude-opus*":   "gpt-5.6-terra",
				"claude-sonnet*": "gpt-5.6-luna",
				"claude-haiku*":  "gpt-5.5",
			},
		},
	}

	tests := map[string]string{
		"claude-fable":             "gpt-5.6-sol",
		"claude-fable-5-20260101":  "gpt-5.6-sol",
		"claude-opus":              "gpt-5.6-terra",
		"claude-opus-5-20260101":   "gpt-5.6-terra",
		"claude-sonnet":            "gpt-5.6-luna",
		"claude-sonnet-5-20260101": "gpt-5.6-luna",
		"claude-haiku":             "gpt-5.5",
		"claude-haiku-5-20260101":  "gpt-5.5",
	}
	for requestedModel, expected := range tests {
		require.Equal(t, expected, group.ResolveMessagesDispatchModel(requestedModel), requestedModel)
	}
}

func TestSanitizeGroupMessagesDispatchFields_ClearsNonOpenAIPlatform(t *testing.T) {
	t.Parallel()

	group := &Group{
		Platform:              PlatformAnthropic,
		AllowMessagesDispatch: true,
		DefaultMappedModel:    "gpt-5.6-sol",
		MessagesDispatchModelConfig: OpenAIMessagesDispatchModelConfig{
			SonnetMappedModel: "gpt-5.3-codex",
			ExactModelMappings: map[string]string{
				"claude-fable-5": "gpt-5.6-sol",
			},
		},
	}

	sanitizeGroupMessagesDispatchFields(group)

	require.False(t, group.AllowMessagesDispatch)
	require.Empty(t, group.DefaultMappedModel)
	require.Equal(t, OpenAIMessagesDispatchModelConfig{}, group.MessagesDispatchModelConfig)
}

func TestSanitizeGroupMessagesDispatchFields_PreservesCompositeFallback(t *testing.T) {
	t.Parallel()

	group := &Group{
		Platform:              PlatformComposite,
		AllowMessagesDispatch: true,
		MessagesDispatchModelConfig: OpenAIMessagesDispatchModelConfig{
			Fallback: &OpenAIMessagesDispatchFallbackConfig{
				Enabled:        true,
				TargetPlatform: PlatformAnthropic,
			},
		},
	}
	sanitizeGroupMessagesDispatchFields(group)
	require.True(t, group.AllowMessagesDispatch)
	require.NotNil(t, group.MessagesDispatchModelConfig.Fallback)
}
