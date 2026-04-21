// Package providers wires STT / LLM / TTS and realtime processors to match Pipecat service names
// under pipecat/src/pipecat/services (see docs/PROVIDERS.md).
package providers

import (
	"context"
	"log"
	"os"
	"strings"

	"google.golang.org/genai"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/services"
	"github.com/rohitdas13595/llmpipe/services/anthropic"
	"github.com/rohitdas13595/llmpipe/services/assemblyai"
	"github.com/rohitdas13595/llmpipe/services/aws"
	"github.com/rohitdas13595/llmpipe/services/deepgram"
	"github.com/rohitdas13595/llmpipe/services/elevenlabs"
	googlesvc "github.com/rohitdas13595/llmpipe/services/google"
	"github.com/rohitdas13595/llmpipe/services/groq"
	"github.com/rohitdas13595/llmpipe/services/openai"
	"github.com/rohitdas13595/llmpipe/services/sarvam"
	"github.com/rohitdas13595/llmpipe/services/whisper"
)

// EnvOr returns os.Getenv(k) or default.
func EnvOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// GenaiClient builds a Gemini API client (same env as Pipecat GenAI).
func GenaiClient(ctx context.Context) (*genai.Client, error) {
	var cfg *genai.ClientConfig
	if k := strings.TrimSpace(os.Getenv("GEMINI_API_KEY")); k != "" {
		cfg = &genai.ClientConfig{APIKey: k}
	} else if k := strings.TrimSpace(os.Getenv("GOOGLE_API_KEY")); k != "" {
		cfg = &genai.ClientConfig{APIKey: k}
	}
	return genai.NewClient(ctx, cfg)
}

// BuildSTT maps STT= env to processors (Pipecat: deepgram/stt, google/stt, openai/stt, groq/stt, assemblyai, …).
func BuildSTT(backend string, re services.ReenterFunc, sampleRate int, bot *aggregate.BotState) processor.Processor {
	b := strings.ToLower(strings.TrimSpace(backend))
	switch b {
	case "google", "gemini":
		gc, err := GenaiClient(context.Background())
		if err != nil {
			log.Fatal("gemini client (STT):", err)
		}
		model := EnvOr("GEMINI_STT_MODEL", EnvOr("GEMINI_MODEL", "gemini-2.0-flash"))
		return googlesvc.NewSTT("stt", gc, model, re, sampleRate, bot)
	case "aws":
		return aws.NewSTT("stt", re)
	case "sarvam":
		return sarvam.NewSTT("stt", re)
	case "openai", "whisper":
		return whisper.NewSTT("stt", whisper.Config{
			BaseURL:    whisper.ResolveBaseURL("https://api.openai.com/v1"),
			APIKey:     os.Getenv("OPENAI_API_KEY"),
			Model:      EnvOr("OPENAI_WHISPER_MODEL", "whisper-1"),
			Language:   os.Getenv("WHISPER_LANGUAGE"),
			SampleRate: sampleRate,
		}, re, sampleRate, bot)
	case "groq":
		return whisper.NewSTT("stt", whisper.Config{
			BaseURL:    EnvOr("GROQ_BASE_URL", whisper.GroqBaseURL),
			APIKey:     os.Getenv("GROQ_API_KEY"),
			Model:      EnvOr("GROQ_STT_MODEL", "whisper-large-v3-turbo"),
			Language:   os.Getenv("WHISPER_LANGUAGE"),
			SampleRate: sampleRate,
		}, re, sampleRate, bot)
	case "assemblyai", "aai":
		return assemblyai.NewSTT("stt", os.Getenv("ASSEMBLYAI_API_KEY"), re, sampleRate, bot)
	default:
		return deepgram.NewSTT("stt", os.Getenv("DEEPGRAM_API_KEY"), re, sampleRate, bot)
	}
}

// BuildLLM maps LLM= (Pipecat: openai/llm, google/llm, anthropic/llm, groq/llm, together, … — see tryLLMOpenAICompat).
func BuildLLM(backend string, re services.ReenterFunc, ctxLLM *aggregate.LLMContext) processor.Processor {
	b := strings.ToLower(strings.TrimSpace(backend))
	if b == "gemini" {
		b = "google"
	}
	if b == "aws" || b == "bedrock" {
		log.Fatal("LLM=aws: AWS Bedrock chat is not implemented in llmpipe; use Pipecat Python (aws/llm.py) or an OpenAI-compatible proxy.")
	}
	if b == "ultravox" {
		log.Fatal("LLM=ultravox: Ultravox uses a dedicated Realtime API; not implemented in llmpipe (see pipecat/services/ultravox/llm.py).")
	}
	switch b {
	case "google":
		gc, err := GenaiClient(context.Background())
		if err != nil {
			log.Fatal("gemini client (LLM):", err)
		}
		model := EnvOr("GEMINI_MODEL", "gemini-2.0-flash")
		log.Printf("LLM backend=google (Gemini) model=%q", model)
		return googlesvc.NewLLM("llm", model, gc, ctxLLM, re)
	case "anthropic", "claude":
		return anthropic.NewLLM("llm", os.Getenv("ANTHROPIC_API_KEY"), EnvOr("ANTHROPIC_MODEL", "claude-3-5-haiku-20241022"), ctxLLM, re)
	case "groq":
		return groq.NewLLM("llm", ctxLLM, re)
	case "azure":
		endpoint := strings.TrimRight(strings.TrimSpace(os.Getenv("AZURE_OPENAI_ENDPOINT")), "/")
		deployment := strings.TrimSpace(os.Getenv("AZURE_OPENAI_DEPLOYMENT"))
		if endpoint == "" || deployment == "" {
			log.Fatal("LLM=azure requires AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_DEPLOYMENT (Pipecat: AzureLLMService).")
		}
		base := endpoint + "/openai/deployments/" + deployment
		apiVer := EnvOr("AZURE_OPENAI_API_VERSION", "2024-08-01-preview")
		model := EnvOr("AZURE_OPENAI_MODEL", deployment)
		l := openai.NewLLMWithBaseURL("llm", os.Getenv("AZURE_OPENAI_API_KEY"), base, model, ctxLLM, re)
		l.APIVersion = apiVer
		return l
	default:
		if p := tryLLMOpenAICompat(b, ctxLLM, re); p != nil {
			return p
		}
		return openai.NewLLM("llm", os.Getenv("OPENAI_API_KEY"), EnvOr("OPENAI_MODEL", "gpt-4o-mini"), ctxLLM, re)
	}
}

// BuildTTS maps TTS= (Pipecat: elevenlabs, openai, google, aws, sarvam, …).
func BuildTTS(backend string, bot *aggregate.BotState, sampleRate int) processor.Processor {
	b := strings.ToLower(strings.TrimSpace(backend))
	switch b {
	case "google":
		return googlesvc.NewTTS("tts")
	case "aws":
		return aws.NewTTS("tts")
	case "sarvam":
		return sarvam.NewTTS("tts", os.Getenv("SARVAM_API_KEY"), bot, sampleRate)
	case "openai":
		return openai.NewTTSService("tts", os.Getenv("OPENAI_API_KEY"), os.Getenv("OPENAI_TTS_VOICE"), bot, sampleRate)
	case "groq":
		// Pipecat: groq/tts.py uses Groq OpenAI-compatible audio.speech (48 kHz WAV in Python; we request pcm like OpenAI).
		return openai.NewTTSServiceWithBaseURL("tts",
			os.Getenv("GROQ_API_KEY"),
			EnvOr("GROQ_BASE_URL", "https://api.groq.com/openai/v1"),
			EnvOr("GROQ_TTS_MODEL", "canopylabs/orpheus-v1-english"),
			EnvOr("GROQ_TTS_VOICE", "autumn"),
			bot, sampleRate)
	default:
		return elevenlabs.NewTTS("tts", os.Getenv("ELEVENLABS_API_KEY"), os.Getenv("ELEVENLABS_VOICE_ID"), bot, sampleRate)
	}
}

// PipelineMode returns PIPELINE= classic | gemini_live | openai_realtime (default classic).
func PipelineMode() string {
	return strings.ToLower(strings.TrimSpace(EnvOr("PIPELINE", "classic")))
}

// BuildGeminiLive for PIPELINE=gemini_live (Pipecat google/gemini_live/llm.py).
func BuildGeminiLive(re services.ReenterFunc, bot *aggregate.BotState, systemPrompt string) processor.Processor {
	gc, err := GenaiClient(context.Background())
	if err != nil {
		log.Fatal("gemini client (live):", err)
	}
	model := EnvOr("GEMINI_LIVE_MODEL", "gemini-2.0-flash-live-preview-04-09")
	return googlesvc.NewGeminiLive("gemini.live", gc, model, systemPrompt, re, bot)
}

// BuildOpenAIRealtime for PIPELINE=openai_realtime (Pipecat openai/realtime/llm.py).
func BuildOpenAIRealtime(re services.ReenterFunc, bot *aggregate.BotState, systemPrompt string) processor.Processor {
	return openai.NewRealtime("openai.realtime", os.Getenv("OPENAI_API_KEY"), os.Getenv("OPENAI_REALTIME_MODEL"), systemPrompt, re, bot)
}
