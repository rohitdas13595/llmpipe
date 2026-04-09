// Command voicebot is a WebSocket PCM demo: input → VAD → STT → LLM → TTS → output.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	ex "github.com/rohitdas13595/llmpipe/examples"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/audio/interrupt"
	"github.com/rohitdas13595/llmpipe/audio/vad"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/observe"
	"github.com/rohitdas13595/llmpipe/pipeline"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/processors/idle"
	"github.com/rohitdas13595/llmpipe/services"
	"github.com/rohitdas13595/llmpipe/services/aws"
	"github.com/rohitdas13595/llmpipe/services/deepgram"
	eleven "github.com/rohitdas13595/llmpipe/services/elevenlabs"
	googlesvc "github.com/rohitdas13595/llmpipe/services/google"
	"github.com/rohitdas13595/llmpipe/services/openai"
	"github.com/rohitdas13595/llmpipe/services/sarvam"
	"github.com/rohitdas13595/llmpipe/transport/ws"

	"google.golang.org/genai"

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	sampleRate := 16000
	if v := os.Getenv("SAMPLE_RATE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			sampleRate = n
		}
	}

	llmBackend := strings.ToLower(envOr("LLM", "openai"))
	if llmBackend == "gemini" {
		llmBackend = "google" // Gemini API via google.golang.org/genai
	}
	sttBackend := strings.ToLower(envOr("STT", "deepgram"))
	ttsBackend := strings.ToLower(envOr("TTS", "elevenlabs"))

	ctxLLM := aggregate.NewLLMContext(envOr("SYSTEM_PROMPT", "You are a concise voice assistant."))
	bot := aggregate.NewBotState()
	strategy := interrupt.MinWords{N: 1}

	var task *pipeline.PipelineTask
	reenter := func(ctx context.Context, name string, f frames.Frame) error {
		if task == nil {
			return nil
		}
		return task.ReenterAfter(ctx, name, f)
	}

	stt := buildSTT(sttBackend, reenter, sampleRate, bot)
	llm := buildLLM(llmBackend, reenter, ctxLLM)
	tts := buildTTS(ttsBackend, bot, sampleRate)

	userAgg := aggregate.NewUserAggregator("user.agg", ctxLLM, bot, strategy)
	asst := aggregate.NewAssistantAggregator("assistant", ctxLLM)
	vadTh := 120.0
	if v := os.Getenv("VAD_RMS_THRESHOLD"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			vadTh = f
		}
	}
	vadP := vad.NewProcessor("vad", vad.NewEnergyAnalyzer(vadTh, envIntOr("VAD_MIN_SPEECH", 2), envIntOr("VAD_MIN_SILENCE", 6)))

	userIdle := idle.NewUserProcessor("user.idle", 2*time.Minute, func(retry int) bool {
		log.Printf("user idle callback retry=%d", retry)
		return retry < 2
	})

	tr := ws.NewTransport(sampleRate, func(ctx context.Context, ff []frames.Frame) error {
		if task == nil {
			return nil
		}
		return task.QueueFrames(ctx, ff)
	})

	procs := []processor.Processor{
		userIdle,
		tr.Input(),
		vadP,
		stt,
		userAgg,
		llm,
		asst,
		tts,
		tr.Output(),
	}

	p := pipeline.NewPipeline(procs...)

	idleObs := observe.NewIdleFrameObserver(observe.IdleConfig{
		Timeout: 30 * time.Minute,
		OnIdle: func() {
			log.Println("pipeline idle timeout")
			if task != nil {
				task.Cancel()
			}
		},
	})

	task = pipeline.NewPipelineTask(p, pipeline.WithIdleObserver(idleObs))

	addr := envOr("LISTEN", ":8080")
	http.Handle("/demo/", http.StripPrefix("/demo/", http.FileServer(http.FS(ex.FS))))
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		if err := tr.HandleWebSocket(w, r); err != nil {
			log.Println("websocket:", err)
		}
	})
	go func() {
		show := addr
		if strings.HasPrefix(show, ":") {
			show = "127.0.0.1" + show
		} else if strings.HasPrefix(show, "0.0.0.0:") {
			show = "127.0.0.1:" + strings.TrimPrefix(show, "0.0.0.0:")
		}
		log.Printf("voicebot: http://%s/demo/voicebot-client.html · ws://%s/ws (%d Hz s16le mono)", show, show, sampleRate)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Fatal(err)
		}
	}()

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runner := pipeline.NewRunner(false)
	if err := runner.Run(rootCtx, task); err != nil && err != context.Canceled {
		log.Println("runner:", err)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envIntOr(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func buildSTT(backend string, re services.ReenterFunc, sampleRate int, bot *aggregate.BotState) processor.Processor {
	switch backend {
	case "google":
		return googlesvc.NewSTT("stt", re)
	case "aws":
		return aws.NewSTT("stt", re)
	case "sarvam":
		return sarvam.NewSTT("stt", re)
	default:
		return deepgram.NewSTT("stt", os.Getenv("DEEPGRAM_API_KEY"), re, sampleRate, bot)
	}
}

func buildLLM(backend string, re services.ReenterFunc, c *aggregate.LLMContext) processor.Processor {
	switch backend {
	case "google":
		var cfg *genai.ClientConfig
		if k := strings.TrimSpace(os.Getenv("GEMINI_API_KEY")); k != "" {
			cfg = &genai.ClientConfig{APIKey: k}
		} else if k := strings.TrimSpace(os.Getenv("GOOGLE_API_KEY")); k != "" {
			cfg = &genai.ClientConfig{APIKey: k}
		}
		gc, err := genai.NewClient(context.Background(), cfg)
		if err != nil {
			log.Fatal("gemini client:", err)
		}
		model := envOr("GEMINI_MODEL", "gemini-2.0-flash")
		log.Printf("LLM backend=google (Gemini) model=%q", model)
		return googlesvc.NewLLM("llm", model, gc, c, re)
	default:
		return openai.NewLLM("llm", os.Getenv("OPENAI_API_KEY"), os.Getenv("OPENAI_MODEL"), c, re)
	}
}

func buildTTS(backend string, bot *aggregate.BotState, sampleRate int) processor.Processor {
	switch backend {
	case "google":
		return googlesvc.NewTTS("tts")
	case "aws":
		return aws.NewTTS("tts")
	case "sarvam":
		return sarvam.NewTTS("tts", os.Getenv("SARVAM_API_KEY"), bot, sampleRate)
	default:
		return eleven.NewTTS("tts", os.Getenv("ELEVENLABS_API_KEY"), os.Getenv("ELEVENLABS_VOICE_ID"), bot, sampleRate)
	}
}
