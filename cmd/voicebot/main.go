// Command voicebot is a WebSocket PCM demo: input → VAD → STT → LLM → TTS → output,
// or PIPELINE=gemini_live / openai_realtime (see ../pipecat Parity in docs/PROVIDERS.md).
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
	"github.com/rohitdas13595/llmpipe/audio/turn"
	"github.com/rohitdas13595/llmpipe/audio/vad"
	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/observe"
	"github.com/rohitdas13595/llmpipe/pipeline"
	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/processors/idle"
	"github.com/rohitdas13595/llmpipe/providers"
	"github.com/rohitdas13595/llmpipe/transport/ws"

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

	llmBackend := strings.ToLower(providers.EnvOr("LLM", "openai"))
	if llmBackend == "gemini" {
		llmBackend = "google"
	}
	sttBackend := strings.ToLower(providers.EnvOr("STT", "deepgram"))
	ttsBackend := strings.ToLower(providers.EnvOr("TTS", "elevenlabs"))
	pipeMode := providers.PipelineMode()

	ctxLLM := aggregate.NewLLMContext(providers.EnvOr("SYSTEM_PROMPT", "You are a concise voice assistant."))
	bot := aggregate.NewBotState()
	strategy := interrupt.MinWords{N: 1}

	var task *pipeline.PipelineTask
	reenter := func(ctx context.Context, name string, f frames.Frame) error {
		if task == nil {
			return nil
		}
		return task.ReenterAfter(ctx, name, f)
	}

	userAgg := aggregate.NewUserAggregator("user.agg", ctxLLM, bot, strategy)
	asst := aggregate.NewAssistantAggregator("assistant", ctxLLM)
	vadTh := 120.0
	if v := os.Getenv("VAD_RMS_THRESHOLD"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			vadTh = f
		}
	}
	vadAna := vad.NewEnergyAnalyzer(vadTh, envIntOr("VAD_MIN_SPEECH", 2), envIntOr("VAD_MIN_SILENCE", 6))
	if v := os.Getenv("TURN_SILENCE_MS"); v != "" {
		if ms, err := strconv.ParseFloat(v, 64); err == nil && ms > 0 {
			vadAna.SilenceStopMS = ms
			log.Printf("VAD: TURN_SILENCE_MS=%.0f (time-based end-of-user-turn)", ms)
		}
	}
	vadP := vad.NewProcessor("vad", vadAna)

	userIdle := idle.NewUserProcessor("user.idle", time.Minute, func(retry int) bool {
		log.Printf("user idle 1m: end pipeline task only (HTTP server keeps running) retry=%d", retry)
		if task != nil {
			_ = task.QueueFrames(context.Background(), []frames.Frame{&frames.EndFrame{}})
			task.Cancel()
		}
		return false
	})

	tr := ws.NewTransport(sampleRate, func(ctx context.Context, ff []frames.Frame) error {
		if task == nil {
			return nil
		}
		return task.QueueFrames(ctx, ff)
	})

	var procs []processor.Processor
	switch pipeMode {
	case "gemini_live", "google_live":
		log.Printf("PIPELINE=%s (Gemini Live — Pipecat google/gemini_live)", pipeMode)
		procs = []processor.Processor{
			userIdle,
			tr.Input(),
			providers.BuildGeminiLive(reenter, bot, providers.EnvOr("SYSTEM_PROMPT", "")),
			tr.Output(),
		}
	case "openai_realtime":
		log.Printf("PIPELINE=openai_realtime (Pipecat openai/realtime)")
		procs = []processor.Processor{
			userIdle,
			tr.Input(),
			providers.BuildOpenAIRealtime(reenter, bot, providers.EnvOr("SYSTEM_PROMPT", "")),
			tr.Output(),
		}
	default:
		stt := providers.BuildSTT(sttBackend, reenter, sampleRate, bot)
		llm := providers.BuildLLM(llmBackend, reenter, ctxLLM)
		tts := providers.BuildTTS(ttsBackend, bot, sampleRate)
		procs = []processor.Processor{
			userIdle,
			tr.Input(),
			vadP,
		}
		if providers.EnvOr("TURN_TRACK", "1") != "0" {
			procs = append(procs, turn.NewTrackingProcessor("turn"))
		}
		procs = append(procs,
			stt,
			userAgg,
			llm,
			asst,
			tts,
			tr.Output(),
		)
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

	tr.OnDisconnect = func() {
		log.Println("websocket closed: queue EndFrame and stop pipeline")
		if task != nil {
			_ = task.QueueFrames(context.Background(), []frames.Frame{&frames.EndFrame{}})
			task.Cancel()
		}
	}

	addr := providers.EnvOr("LISTEN", ":8080")
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
		log.Printf("voicebot: http://%s/demo/voicebot-client.html · ws://%s/ws (%d Hz s16le mono) PIPELINE=%s",
			show, show, sampleRate, pipeMode)
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
	log.Println("pipeline task stopped; HTTP server still listening until SIGINT/SIGTERM")
	<-rootCtx.Done()
}

func envIntOr(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}
