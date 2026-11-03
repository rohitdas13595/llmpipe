// Command voicebot is a WebSocket PCM demo: input → VAD → turns → STT → LLM → TTS → output,
// or PIPELINE=gemini_live / openai_realtime (see ../pipecat Parity in docs/PROVIDERS.md).
// Each WebSocket connection gets its own pipeline task.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	ex "github.com/rohitdas13595/llmpipe/examples"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/audio/interrupt"
	"github.com/rohitdas13595/llmpipe/audio/turn"
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

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var sessions sessionSet
	mux := http.NewServeMux()
	mux.Handle("/demo/", http.StripPrefix("/demo/", http.FileServer(http.FS(ex.FS))))
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		runVoiceSession(rootCtx, &sessions, w, r, sampleRate)
	})

	addr := providers.EnvOr("LISTEN", ":8080")
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		show := addr
		if strings.HasPrefix(show, ":") {
			show = "127.0.0.1" + show
		} else if strings.HasPrefix(show, "0.0.0.0:") {
			show = "127.0.0.1:" + strings.TrimPrefix(show, "0.0.0.0:")
		}
		log.Printf("voicebot: http://%s/demo/voicebot-client.html · ws://%s/ws (%d Hz s16le mono) PIPELINE=%s",
			show, show, sampleRate, providers.PipelineMode())
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-rootCtx.Done()
	sessions.cancelAll()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Println("http shutdown:", err)
	}
}

type sessionSet struct {
	mu    sync.Mutex
	tasks []*pipeline.PipelineTask
}

func (s *sessionSet) add(t *pipeline.PipelineTask) {
	s.mu.Lock()
	s.tasks = append(s.tasks, t)
	s.mu.Unlock()
}

func (s *sessionSet) cancelAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.tasks {
		t.Cancel()
	}
}

func runVoiceSession(root context.Context, sessions *sessionSet, w http.ResponseWriter, r *http.Request, sampleRate int) {
	llmBackend := strings.ToLower(providers.EnvOr("LLM", "openai"))
	if llmBackend == "gemini" {
		llmBackend = "google"
	}
	sttBackend := strings.ToLower(providers.EnvOr("STT", "deepgram"))
	ttsBackend := strings.ToLower(providers.EnvOr("TTS", "elevenlabs"))
	pipeMode := providers.PipelineMode()

	reg := providers.ToolRegistryFromEnv()
	ctxLLM := aggregate.NewLLMContext(providers.WithToolPrompt(providers.EnvOr("SYSTEM_PROMPT", "You are a concise voice assistant.")))
	bot := aggregate.NewBotState()
	strategy := interrupt.MinWords{N: 1}

	var task *pipeline.PipelineTask
	tr := ws.NewTransport(sampleRate, func(ctx context.Context, ff []frames.Frame) error {
		if task == nil {
			return nil
		}
		return task.QueueFrames(ctx, ff)
	})

	userIdle := idle.NewUserProcessor("user.idle", time.Minute, func(retry int) bool {
		log.Printf("user idle 1m: end pipeline task only (HTTP server keeps running) retry=%d", retry)
		if task != nil {
			task.StopWhenDone()
		}
		return false
	})

	var procs []processor.Processor
	switch pipeMode {
	case "gemini_live", "google_live":
		log.Printf("PIPELINE=%s (Gemini Live — Pipecat google/gemini_live)", pipeMode)
		live := providers.BuildGeminiLive(nil, bot, providers.WithToolPrompt(providers.EnvOr("SYSTEM_PROMPT", "")))
		providers.AttachTools(live, reg)
		procs = []processor.Processor{
			userIdle,
			tr.Input(),
			live,
			tr.Output(),
		}
	case "openai_realtime":
		log.Printf("PIPELINE=openai_realtime (Pipecat openai/realtime)")
		rt := providers.BuildOpenAIRealtime(nil, bot, providers.WithToolPrompt(providers.EnvOr("SYSTEM_PROMPT", "")))
		providers.AttachTools(rt, reg)
		procs = []processor.Processor{
			userIdle,
			tr.Input(),
			rt,
			tr.Output(),
		}
	default:
		stt := providers.BuildSTT(sttBackend, nil, sampleRate, bot)
		llm := providers.BuildLLM(llmBackend, nil, ctxLLM)
		providers.AttachTools(llm, reg)
		tts := providers.BuildTTS(ttsBackend, bot, sampleRate)
		procs = []processor.Processor{
			userIdle,
			tr.Input(),
			providers.BuildVAD(sampleRate),
			providers.BuildUserTurns(),
		}
		if providers.EnvOr("TURN_TRACK", "1") != "0" {
			procs = append(procs, turn.NewTrackingProcessor("turn"))
		}
		procs = append(procs,
			stt,
			aggregate.NewUserAggregator("user.agg", ctxLLM, bot, strategy),
			llm,
			tts,
			tr.Output(),
			aggregate.NewAssistantAggregator("assistant", ctxLLM),
		)
	}

	idleObs := observe.NewIdleFrameObserver(observe.IdleConfig{
		Timeout: 30 * time.Minute,
		OnIdle: func() {
			log.Println("pipeline idle timeout")
			if task != nil {
				task.StopWhenDone()
			}
		},
	})
	task = pipeline.NewPipelineTask(pipeline.NewPipeline(procs...), pipeline.WithIdleObserver(idleObs))
	sessions.add(task)
	tr.OnDisconnect = func() {
		log.Println("websocket closed: cancel pipeline task")
		task.Cancel()
	}

	ctx, cancel := context.WithCancel(root)
	defer cancel()
	go func() {
		select {
		case <-r.Context().Done():
			cancel()
		case <-ctx.Done():
		}
	}()
	errCh := make(chan error, 1)
	go func() { errCh <- task.Run(ctx) }()
	if err := tr.HandleWebSocket(w, r); err != nil {
		log.Println("websocket:", err)
	}
	task.Cancel()
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			log.Println("pipeline:", err)
		}
	case <-time.After(25 * time.Second):
		log.Println("pipeline cancel timed out")
	}
}
