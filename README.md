# A Linear frame pipeline: transport **Input()/Output()** processors, **VAD**, **STT → LLM → TTS**, aggregators, **interruption**, **pipeline/user idle**, and pluggable **FrameObserver** (including **IdleFrameObserver**).

**Module:** `github.com/rohitdas13595/llmpipe` — run `go get github.com/rohitdas13595/llmpipe@latest`. Repository: [https://github.com/rohitdas13595/llmpipe](https://github.com/rohitdas13595/llmpipe). For a local checkout, use a `replace` directive (see `docs/REFERENCE.md`).

## Documentation

Files in [`docs/`](docs/):

| Document | Description |
| -------- | ----------- |
| [`REFERENCE.md`](docs/REFERENCE.md) | Full architecture and API reference for the module |
| [`PROVIDERS.md`](docs/PROVIDERS.md) | Pipecat ↔ llmpipe provider mapping and env vars |

**[`docs/REFERENCE.md`](docs/REFERENCE.md)** covers:

- Pipeline mental model, frame flow, and **async `ReenterAfter`** for STT/LLM
- Package map, core types (`Pipeline`, `PipelineTask`, `Runner`, `Processor`, frames)
- Aggregators, VAD, interruption, observers, transports (WebSocket & LiveKit)
- Provider summary and **how to use this module in another project** (`go get`, optional `go.mod` `replace`, minimal skeleton, custom processors)

## Layout

- `frames` — frame types (`StartFrame`, `InputAudioRawFrame`, `TranscriptionFrame`, `LLMRunFrame`, `InterruptionFrame`, …)
- `processor`, `pipeline` — `Pipeline`, `PipelineTask` (`QueueFrames`, `Reenter` / `ReenterAfter`), `Runner`
- `observe` — `FrameObserver`, `IdleFrameObserver`
- `aggregate` — `LLMContext`, user/assistant aggregators
- `audio/vad`, `audio/interrupt`, `audio/turn` (stub for smart-turn)
- `services` — provider STT/LLM/TTS + realtime (`openai`, `google`, `anthropic`, `groq`, `whisper`, `assemblyai`, …); see [`docs/PROVIDERS.md`](docs/PROVIDERS.md) for parity with the Python [`pipecat`](../pipecat) tree
- `providers` — env-based wiring (`providers.BuildSTT` / `BuildLLM` / `BuildTTS`, `PIPELINE=gemini_live|openai_realtime`)
- `transport/ws`, `transport/livekit` — WebSocket PCM and LiveKit room transport

## Environment

Copy the template and edit secrets (run commands from the `llmpipe/` directory so `.env` is found):

```bash
cp .env.example .env
```

Both `**cmd/voicebot**` and `**cmd/voicebot-livekit**` call `godotenv.Load()` at startup (missing `.env` is ignored). The committed `**.env.example**` lists every variable; `**.env**` is gitignored.

**WebSocket demo** (`go run ./cmd/voicebot`):


| Variable                                    | Purpose                                                                            |
| ------------------------------------------- | ---------------------------------------------------------------------------------- |
| `LISTEN`                                    | HTTP address (default `:8080`)                                                     |
| `SAMPLE_RATE`                               | PCM rate (default `16000`)                                                         |
| `PIPELINE`                                  | `classic` (default), `gemini_live`, `openai_realtime` — realtime modes skip VAD+STT+LLM+TTS (see [`docs/PROVIDERS.md`](docs/PROVIDERS.md)) |
| `STT`                                       | `deepgram` (default), `google`, `openai`, `groq`, `assemblyai`, `aws`, `sarvam`     |
| `LLM`                                       | `openai` (default), `google`, `anthropic`, `groq`                                 |
| `TTS`                                       | `elevenlabs` (default), `openai`, `google`, `aws`, `sarvam`                         |
| `OPENAI_API_KEY`, `OPENAI_MODEL`, `OPENAI_BASE_URL` | Chat completions; base URL optional (OpenAI-compatible providers)          |
| `DEEPGRAM_API_KEY`                          | Deepgram prerecorded (on utterance end)                                            |
| `ELEVENLABS_API_KEY`, `ELEVENLABS_VOICE_ID` | ElevenLabs PCM                                                                     |
| `GEMINI_MODEL`, `GEMINI_API_KEY` / `GOOGLE_API_KEY` | Gemini LLM; for `STT=google` also short audio transcription (`GEMINI_STT_MODEL`) |
| `GROQ_API_KEY`, `GROQ_MODEL`                | Groq LLM; `STT=groq` uses Whisper on Groq                                           |
| `ANTHROPIC_API_KEY`, `ANTHROPIC_MODEL`      | Claude when `LLM=anthropic`                                                         |
| `ASSEMBLYAI_API_KEY`                        | When `STT=assemblyai`                                                               |
| `SYSTEM_PROMPT`                             | System message / realtime instructions                                             |


Clients send **binary WebSocket messages** of **16-bit little-endian mono PCM**; bot replies with PCM chunks.

### Browser demo (examples)

With the server running, open:

**[http://127.0.0.1:8080/demo/voicebot-client.html](http://127.0.0.1:8080/demo/voicebot-client.html)**

(or **[http://127.0.0.1:8080/demo/](http://127.0.0.1:8080/demo/)** for an index). The page is embedded via `go:embed` from `examples/`. It captures the microphone, resamples to the configured rate (default **16000** Hz — must match `SAMPLE_RATE`), streams PCM over `/ws`, and plays bot TTS chunks. Optional **push-to-talk** avoids sending ambient noise.

Static copies of the same HTML also live under `examples/` if you want to open or host them separately.

**Smaller Go examples**

| Command / test | What it shows |
|----------------|----------------|
| `go run ./cmd/example-minimal-pipeline` | Single processor + `FrameObserver`, no APIs |
| `go test -v -run '^Example' ./examples` | `Example*` snippets: `transcriptions`, `serializers`, WhatsApp webhook helpers |

**LiveKit** (`go run ./cmd/voicebot-livekit`):


| Variable                                               | Purpose               |
| ------------------------------------------------------ | --------------------- |
| `LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET` | Room connection       |
| `LIVEKIT_ROOM`, `LIVEKIT_IDENTITY`                     | Room and bot identity |
| Same `STT` / `LLM` / `TTS` / provider keys as above    |                       |


The bot publishes an **Opus** track decoded/encoded via the LiveKit SDK; internal pipeline uses the configured `SAMPLE_RATE` (default 16 kHz).

With the agent running, open **[http://127.0.0.1:8090/demo/livekit-voicebot-client.html](http://127.0.0.1:8090/demo/livekit-voicebot-client.html)** (default `LIVEKIT_DEMO_LISTEN=:8090`) to join the same room from the browser; the page calls **`GET /api/livekit-token`** to mint a viewer token. Set **`LIVEKIT_DEMO_LISTEN=off`** to turn off that HTTP server.

**Connection timeouts or `i/o timeout` to `*.livekit.cloud:443`:** the agent uses a **30s** signalling timeout by default (override with **`LIVEKIT_CONNECT_TIMEOUT`**). If your network blocks some LiveKit edges, try **`LIVEKIT_DISABLE_REGION_DISCOVERY=1`** so only **`LIVEKIT_URL`** is used. You still need outbound **HTTPS/WSS on 443** and working **WebRTC** (UDP or TURN); restrictive firewalls and broken IPv6 can block TURN—try another network or VPN, or **`LIVEKIT_ICE_TRANSPORT=relay`**.

## Tests

```bash
go test ./...
```

## License

This project is licensed under the [GNU Affero General Public License v3.0](LICENSE). See [CONTRIBUTING.md](CONTRIBUTING.md) for how to contribute.

