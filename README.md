# A Linear frame pipeline: transport **Input()/Output()** processors, **VAD**, **STT → LLM → TTS**, aggregators, **interruption**, **pipeline/user idle**, and pluggable **FrameObserver** (including **IdleFrameObserver**).

**Module:** `github.com/rohitdas13595/llmpipe` — run `go get github.com/rohitdas13595/llmpipe@latest`. Repository: [https://github.com/rohitdas13595/llmpipe](https://github.com/rohitdas13595/llmpipe). For a local checkout, use a `replace` directive (see `docs/REFERENCE.md`).

## Documentation

**[Full architecture & API reference → `docs/REFERENCE.md](docs/REFERENCE.md)`** — detailed guide covering:

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
- `services` — `LLM` / `STT` / `TTS` style processors; OpenAI + Deepgram + ElevenLabs MVP; Google / AWS / Sarvam stubs
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
| `STT`                                       | `deepgram` (default), `google`, `aws`, `sarvam`                                    |
| `LLM`                                       | `openai` (default), `google`                                                       |
| `TTS`                                       | `elevenlabs` (default), `google`, `aws`, `sarvam`                                  |
| `OPENAI_API_KEY`, `OPENAI_MODEL`            | OpenAI chat                                                                        |
| `DEEPGRAM_API_KEY`                          | Deepgram prerecorded (on utterance end)                                            |
| `ELEVENLABS_API_KEY`, `ELEVENLABS_VOICE_ID` | ElevenLabs PCM                                                                     |
| `GEMINI_MODEL`                              | Optional; Google LLM uses default client env (`GEMINI_API_KEY` / `GOOGLE_API_KEY`) |
| `SYSTEM_PROMPT`                             | System message                                                                     |


Clients send **binary WebSocket messages** of **16-bit little-endian mono PCM**; bot replies with PCM chunks.

### Browser demo (examples)

With the server running, open:

**[http://127.0.0.1:8080/demo/voicebot-client.html](http://127.0.0.1:8080/demo/voicebot-client.html)**

(or **[http://127.0.0.1:8080/demo/](http://127.0.0.1:8080/demo/)** for an index). The page is embedded via `go:embed` from `examples/`. It captures the microphone, resamples to the configured rate (default **16000** Hz — must match `SAMPLE_RATE`), streams PCM over `/ws`, and plays bot TTS chunks. Optional **push-to-talk** avoids sending ambient noise.

Static copies of the same HTML also live under `examples/` if you want to open or host them separately.

**LiveKit** (`go run ./cmd/voicebot-livekit`):


| Variable                                               | Purpose               |
| ------------------------------------------------------ | --------------------- |
| `LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET` | Room connection       |
| `LIVEKIT_ROOM`, `LIVEKIT_IDENTITY`                     | Room and bot identity |
| Same `STT` / `LLM` / `TTS` / provider keys as above    |                       |


The bot publishes an **Opus** track decoded/encoded via the LiveKit SDK; internal pipeline uses the configured `SAMPLE_RATE` (default 16 kHz).

## Tests

```bash
go test ./...
```

