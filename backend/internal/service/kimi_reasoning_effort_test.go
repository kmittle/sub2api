package service

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestMapKimiReasoningEffortPreservesOrdinalOrder(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{raw: "minimal", want: "low"},
		{raw: "low", want: "low"},
		{raw: "medium", want: "low"},
		{raw: "high", want: "low"},
		{raw: "x-high", want: "high"},
		{raw: "max", want: "max"},
		{raw: "unknown", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			require.Equal(t, tt.want, mapKimiReasoningEffort(tt.raw))
		})
	}
}

func TestMapKimiOpenAIReasoningEffortForGPT56UsesPracticalCLIOrder(t *testing.T) {
	require.Equal(t, "low", mapKimiOpenAIReasoningEffortForModel("low", "gpt-5.6-sol"))
	require.Equal(t, "low", mapKimiOpenAIReasoningEffortForModel("medium", "gpt-5.6-sol"))
	require.Equal(t, "high", mapKimiOpenAIReasoningEffortForModel("high", "gpt-5.6-sol"))
	require.Equal(t, "max", mapKimiOpenAIReasoningEffortForModel("max", "gpt-5.6-sol"))
}

func TestMapKimiAnthropicEffortPreservesClaudeOrder(t *testing.T) {
	require.Equal(t, "low", mapKimiAnthropicEffort("low"))
	require.Equal(t, "low", mapKimiAnthropicEffort("medium"))
	require.Equal(t, "high", mapKimiAnthropicEffort("high"))
	require.Equal(t, "max", mapKimiAnthropicEffort("max"))
}

func TestNormalizeKimiReasoningEffortBody(t *testing.T) {
	body := []byte(`{"reasoning":{"effort":"max"},"reasoning_effort":"medium"}`)
	got, changed := normalizeKimiOpenAIReasoningEffortBody(body, "kimi-k3", "gpt-5.6-sol")
	require.True(t, changed)
	require.Equal(t, "max", gjson.GetBytes(got, "reasoning.effort").String())
	require.Equal(t, "low", gjson.GetBytes(got, "reasoning_effort").String())

	native := extractKimiNativeReasoningEffortFromBody(got)
	require.NotNil(t, native)
	require.Equal(t, "max", *native)

	nativeBody := []byte(`{"model":"kimi-k3","reasoning_effort":"high"}`)
	normalized, changed := normalizeKimiOpenAIReasoningEffortBody(nativeBody, "kimi-k3", "kimi-k3")
	require.True(t, changed == false)
	require.Equal(t, "high", gjson.GetBytes(normalized, "reasoning_effort").String())

	unchanged, changed := normalizeKimiOpenAIReasoningEffortBody(body, "gpt-5.6-sol", "gpt-5.6-sol")
	require.False(t, changed)
	require.Equal(t, body, unchanged)
}

func TestIsKimiModel(t *testing.T) {
	require.True(t, isKimiModel("kimi-k3"))
	require.True(t, isKimiModel("moonshot/kimi-k3"))
	require.True(t, isKimiModel("k3-256k"))
	require.False(t, isKimiModel("gpt-5.6-sol"))
}
