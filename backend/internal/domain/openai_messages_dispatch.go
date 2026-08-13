package domain

// OpenAIMessagesDispatchFallbackConfig controls the optional cross-platform
// retry for a composite group's Anthropic /v1/messages dispatch.
//
// The fallback intentionally carries only routing authority. Model capability
// and model aliases remain owned by the selected fallback account, so a
// fallback cannot silently substitute a different model family.
type OpenAIMessagesDispatchFallbackConfig struct {
	Enabled        bool   `json:"enabled,omitempty"`
	TargetPlatform string `json:"target_platform,omitempty"`
}

// OpenAIMessagesDispatchModelConfig controls how Anthropic /v1/messages
// requests are mapped onto OpenAI/Codex models and, optionally, where a
// failed OpenAI dispatch may be retried.
type OpenAIMessagesDispatchModelConfig struct {
	OpusMappedModel    string                                `json:"opus_mapped_model,omitempty"`
	SonnetMappedModel  string                                `json:"sonnet_mapped_model,omitempty"`
	HaikuMappedModel   string                                `json:"haiku_mapped_model,omitempty"`
	ExactModelMappings map[string]string                     `json:"exact_model_mappings,omitempty"`
	Fallback           *OpenAIMessagesDispatchFallbackConfig `json:"fallback,omitempty"`
}
