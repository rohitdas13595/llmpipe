// Command voicebot-livekit runs the same voice pipeline with LiveKit Input/Output transport.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/audio/interrupt"
	"github.com/rohitdas13595/llmpipe/audio/vad"
	ex "github.com/rohitdas13595/llmpipe/examples"
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
	lktransport "github.com/rohitdas13595/llmpipe/transport/livekit"

	"github.com/livekit/protocol/auth"
	protoLogger "github.com/livekit/protocol/logger"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"google.golang.org/genai"

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	protoLogger.InitFromConfig(&protoLogger.Config{Level: "warn"}, "voicebot-livekit")
	lksdk.SetLogger(protoLogger.GetLogger())

	url := envOr("LIVEKIT_URL", "ws://localhost:7880")
	room := envOr("LIVEKIT_ROOM", "demo")
	identity := envOr("LIVEKIT_IDENTITY", "llmpipe-agent")
	apiKey := os.Getenv("LIVEKIT_API_KEY")
	apiSecret := os.Getenv("LIVEKIT_API_SECRET")
	if apiKey == "" || apiSecret == "" {
		log.Fatal("LIVEKIT_API_KEY and LIVEKIT_API_SECRET are required")
	}

	demoAddr := strings.TrimSpace(os.Getenv("LIVEKIT_DEMO_LISTEN"))
	if demoAddr == "" {
		demoAddr = ":8090"
	}
	if !strings.EqualFold(demoAddr, "off") && demoAddr != "-" {
		go startLiveKitDemoServer(demoAddr, url, room, apiKey, apiSecret)
	}

	sampleRate := 16000
	if v := os.Getenv("SAMPLE_RATE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			sampleRate = n
		}
	}

	llmBackend := strings.ToLower(envOr("LLM", "openai"))
	if llmBackend == "gemini" {
		llmBackend = "google"
	}
	sttBackend := strings.ToLower(envOr("STT", "deepgram"))
	ttsBackend := strings.ToLower(envOr("TTS", "elevenlabs"))

	ctxLLM := aggregate.NewLLMContext(envOr("SYSTEM_PROMPT", "You are a concise voice assistant."))
	bot := aggregate.NewBotState()
	strategy := interrupt.MinWords{N: 1}

	var task *pipeline.PipelineTask
	var endOnce sync.Once
	endPipeline := func(reason string) {
		endOnce.Do(func() {
			log.Printf("end pipeline: %s", reason)
			if task != nil {
				_ = task.QueueFrames(context.Background(), []frames.Frame{&frames.EndFrame{}})
				task.Cancel()
			}
		})
	}

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

	userIdle := idle.NewUserProcessor("user.idle", time.Minute, func(retry int) bool {
		endPipeline(fmt.Sprintf("user idle 1m — pipeline task only, demo/LiveKit process keeps running (retry=%d)", retry))
		return false
	})

	tr := lktransport.NewTransport(url, lksdk.ConnectInfo{
		APIKey:              apiKey,
		APISecret:           apiSecret,
		RoomName:            room,
		ParticipantIdentity: identity,
	}, sampleRate, func(ctx context.Context, ff []frames.Frame) error {
		if task == nil {
			return nil
		}
		return task.QueueFrames(ctx, ff)
	}, protoLogger.GetLogger(), lktransport.ConnectOptionsFromEnv()...)

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

	tr.OnDisconnect = func() {
		endPipeline("livekit remote participant left or room disconnected")
	}

	if err := tr.Connect(context.Background()); err != nil {
		log.Fatal("livekit connect:", err)
	}
	defer tr.Disconnect()

	log.Printf("llmpipe: livekit room joined room=%q agent=%q sampleRate=%d (Pion ICE/TURN lines are WebRTC, not frame pipeline; STT/LLM/TTS are silent unless they error)", room, identity, sampleRate)

	_ = task.QueueFrames(context.Background(), []frames.Frame{
		&frames.StartFrame{SampleRate: sampleRate, NumChannels: 1},
	})
	log.Printf("llmpipe: StartFrame queued — waiting for remote participant + Opus track before audio reaches VAD/STT")

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runner := pipeline.NewRunner(false)
	if err := runner.Run(rootCtx, task); err != nil && err != context.Canceled {
		log.Println("runner:", err)
	}
	log.Println("pipeline task stopped; process still running (demo HTTP + LiveKit) until SIGINT/SIGTERM")
	<-rootCtx.Done()
}

func startLiveKitDemoServer(addr, livekitURL, defaultRoom, apiKey, apiSecret string) {
	mux := http.NewServeMux()
	mux.Handle("/demo/", http.StripPrefix("/demo/", http.FileServer(http.FS(ex.FS))))
	mux.HandleFunc("/api/livekit-token", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		roomName := strings.TrimSpace(r.URL.Query().Get("room"))
		if roomName == "" {
			roomName = defaultRoom
		}
		identity := strings.TrimSpace(r.URL.Query().Get("identity"))
		if identity == "" {
			identity = fmt.Sprintf("web-%d", time.Now().UnixNano())
		}
		at := auth.NewAccessToken(apiKey, apiSecret).
			SetIdentity(identity).
			SetValidFor(6 * time.Hour).
			SetVideoGrant(&auth.VideoGrant{
				RoomJoin: true,
				Room:     roomName,
			})
		token, err := at.ToJWT()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			ServerURL string `json:"serverUrl"`
			Token     string `json:"token"`
			Room      string `json:"room"`
			Identity  string `json:"identity"`
		}{ServerURL: livekitURL, Token: token, Room: roomName, Identity: identity})
	})
	show := addr
	if strings.HasPrefix(show, ":") {
		show = "127.0.0.1" + show
	} else if strings.HasPrefix(show, "0.0.0.0:") {
		show = "127.0.0.1:" + strings.TrimPrefix(show, "0.0.0.0:")
	}
	log.Printf("livekit demo UI http://%s/demo/livekit-voicebot-client.html · GET /api/livekit-token", show)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal("livekit demo http:", err)
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
