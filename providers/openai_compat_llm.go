package providers

import (
	"os"
	"strings"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
	"github.com/rohitdas13595/llmpipe/services/openai"
)

type openAILLMRow struct {
	ids          []string
	keyEnvs      []string
	baseEnv      string
	baseDefault  string
	modelEnvs    []string
	modelDefault string
	// dummyKeyOllama uses "ollama" when no key (matches Pipecat Ollama client).
	dummyKeyOllama bool
}

// Rows match Pipecat OpenAI-derived LLM services under pipecat/src/pipecat/services/*/llm.py
// (BaseOpenAILLMService + OpenAI-compatible /v1/chat/completions).
var openAILLMCompatRows = []openAILLMRow{
	{[]string{"together"}, []string{"TOGETHER_API_KEY"}, "TOGETHER_BASE_URL", "https://api.together.xyz/v1", []string{"TOGETHER_MODEL"}, "meta-llama/Llama-3.3-70B-Instruct-Turbo", false},
	{[]string{"xai", "grok"}, []string{"XAI_API_KEY", "GROK_API_KEY", "XAI_GROK_API_KEY"}, "XAI_BASE_URL", "https://api.x.ai/v1", []string{"XAI_MODEL", "GROK_MODEL"}, "grok-2-1212", false},
	{[]string{"openrouter"}, []string{"OPENROUTER_API_KEY"}, "OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1", []string{"OPENROUTER_MODEL"}, "openai/gpt-4o-2024-11-20", false},
	{[]string{"deepseek"}, []string{"DEEPSEEK_API_KEY"}, "DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1", []string{"DEEPSEEK_MODEL"}, "deepseek-chat", false},
	{[]string{"mistral"}, []string{"MISTRAL_API_KEY"}, "MISTRAL_BASE_URL", "https://api.mistral.ai/v1", []string{"MISTRAL_MODEL"}, "mistral-small-latest", false},
	{[]string{"cerebras"}, []string{"CEREBRAS_API_KEY"}, "CEREBRAS_BASE_URL", "https://api.cerebras.ai/v1", []string{"CEREBRAS_MODEL"}, "gpt-oss-120b", false},
	{[]string{"fireworks", "fireworks_ai"}, []string{"FIREWORKS_API_KEY"}, "FIREWORKS_BASE_URL", "https://api.fireworks.ai/inference/v1", []string{"FIREWORKS_MODEL"}, "accounts/fireworks/models/firefunction-v2", false},
	{[]string{"nvidia"}, []string{"NVIDIA_API_KEY", "NV_API_KEY"}, "NVIDIA_BASE_URL", "https://integrate.api.nvidia.com/v1", []string{"NVIDIA_MODEL"}, "nvidia/llama-3.1-nemotron-70b-instruct", false},
	{[]string{"novita"}, []string{"NOVITA_API_KEY"}, "NOVITA_BASE_URL", "https://api.novita.ai/openai", []string{"NOVITA_MODEL"}, "moonshotai/kimi-k2.5", false},
	{[]string{"nebius"}, []string{"NEBIUS_API_KEY"}, "NEBIUS_BASE_URL", "https://api.tokenfactory.nebius.com/v1", []string{"NEBIUS_MODEL"}, "openai/gpt-oss-120b", false},
	{[]string{"sambanova"}, []string{"SAMBANOVA_API_KEY"}, "SAMBANOVA_BASE_URL", "https://api.sambanova.ai/v1", []string{"SAMBANOVA_MODEL"}, "Llama-4-Maverick-17B-128E-Instruct", false},
	{[]string{"perplexity"}, []string{"PERPLEXITY_API_KEY"}, "PERPLEXITY_BASE_URL", "https://api.perplexity.ai/v1", []string{"PERPLEXITY_MODEL"}, "sonar", false},
	{[]string{"qwen", "dashscope"}, []string{"DASHSCOPE_API_KEY", "QWEN_API_KEY"}, "QWEN_BASE_URL", "https://dashscope-intl.aliyuncs.com/compatible-mode/v1", []string{"QWEN_MODEL"}, "qwen-plus", false},
	{[]string{"sarvam"}, []string{"SARVAM_API_KEY"}, "SARVAM_BASE_URL", "https://api.sarvam.ai/v1", []string{"SARVAM_LLM_MODEL", "SARVAM_MODEL"}, "sarvam-30b", false},
	{[]string{"ollama"}, []string{"OLLAMA_API_KEY"}, "OLLAMA_BASE_URL", "http://127.0.0.1:11434/v1", []string{"OLLAMA_MODEL"}, "llama3.2", true},
}

func firstNonEmptyEnv(keys []string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

// tryLLMOpenAICompat returns an OpenAI-compatible chat LLM for Pipecat backend names, or nil.
func tryLLMOpenAICompat(backend string, ctx *aggregate.LLMContext, re services.ReenterFunc) processor.Processor {
	b := strings.ToLower(strings.TrimSpace(backend))
	for _, row := range openAILLMCompatRows {
		for _, id := range row.ids {
			if id != b {
				continue
			}
			key := firstNonEmptyEnv(row.keyEnvs)
			if key == "" && row.dummyKeyOllama {
				key = "ollama"
			}
			base := EnvOr(row.baseEnv, row.baseDefault)
			base = strings.TrimSuffix(strings.TrimSpace(base), "/")
			model := firstNonEmptyEnv(row.modelEnvs)
			if model == "" {
				model = row.modelDefault
			}
			return openai.NewLLMWithBaseURL("llm", key, base, model, ctx, re)
		}
	}
	return nil
}
