# Pipecat parity (Python) → llmpipe (Go)

The Python reference implementation lives in this repo under `[../pipecat/src/pipecat/services/](../pipecat/src/pipecat/services/)`. The Go module maps the **same provider names** where possible; differences are noted below.

## Classic pipeline (`PIPELINE` unset or `classic`)

Chain: **VAD → STT → user agg → LLM → assistant agg → TTS** (see `cmd/voicebot` / `cmd/voicebot-livekit`).


| Env `STT`            | Pipecat package                           | llmpipe implementation                                                                                |
| -------------------- | ----------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `deepgram` (default) | `deepgram/stt.py`                         | `services/deepgram` — prerecorded HTTP                                                                |
| `google` / `gemini`  | `google/stt.py` (Cloud Speech v2)         | `services/google` — **Gemini multimodal** transcribe (WAV + `GenerateContent`; uses `GEMINI_API_KEY`) |
| `openai` / `whisper` | `openai/stt.py` (`BaseWhisperSTTService`) | `services/whisper` — OpenAI `/v1/audio/transcriptions`                                                |
| `groq`               | `groq/stt.py`                             | `services/whisper` with `GROQ_BASE_URL` default `https://api.groq.com/openai/v1`                      |
| `assemblyai` / `aai` | `assemblyai/stt.py` (streaming WS)        | `services/assemblyai` — **REST** upload + poll per utterance                                          |
| `aws`                | `aws/stt.py`                              | `services/aws` — still a pass-through stub                                                            |
| `sarvam`             | `sarvam/stt.py`                           | `services/sarvam` — stub                                                                              |


Other Pipecat STT backends (`speechmatics`, `cartesia`, `gladia`, `mistral`, `nvidia` Riva, `azure`, …) are **not** wired in llmpipe; use Pipecat Python or extend `providers.BuildSTT`.

### LLM (`LLM=`)


| Kind                    | Env examples                                                                                           | llmpipe                                                                                           |
| ----------------------- | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| OpenAI                  | `OPENAI_API_KEY`, `OPENAI_MODEL`, optional `OPENAI_BASE_URL`                                           | `services/openai` — streaming `/v1/chat/completions`                                              |
| Google Gemini           | `GEMINI_API_KEY`, `GEMINI_MODEL`                                                                       | `services/google` — `GenerateContentStream`                                                       |
| Anthropic               | `ANTHROPIC_API_KEY`, `ANTHROPIC_MODEL`                                                                 | `services/anthropic` — Messages API                                                               |
| Groq                    | `GROQ_API_KEY`, `GROQ_MODEL`                                                                           | `services/groq` — OpenAI-compatible `https://api.groq.com/openai/v1`                              |
| Azure OpenAI            | `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_DEPLOYMENT`, `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_API_VERSION` | `services/openai` — deployment URL + `api-version` query on chat (Pipecat: `azure/llm.py`)        |
| **OpenAI-compat table** | Per-provider `*_API_KEY`, `*_BASE_URL`, `*_MODEL`                                                      | `[openai_compat_llm.go](../providers/openai_compat_llm.go)` → `services/openai` (see table below) |


**OpenAI-compatible LLM backends** (Pipecat `BaseOpenAILLMService` — same Chat Completions API). Implemented in `[../providers/openai_compat_llm.go](../providers/openai_compat_llm.go)`:


| `LLM=`                      | Pipecat             | Key env (first wins)                               | Base default                             |
| --------------------------- | ------------------- | -------------------------------------------------- | ---------------------------------------- |
| `together`                  | `together/llm.py`   | `TOGETHER_API_KEY`                                 | `https://api.together.xyz/v1`            |
| `xai`, `grok`               | `xai/llm.py`        | `XAI_API_KEY`, `GROK_API_KEY`                      | `https://api.x.ai/v1`                    |
| `openrouter`                | `openrouter/llm.py` | `OPENROUTER_API_KEY`                               | `https://openrouter.ai/api/v1`           |
| `deepseek`                  | `deepseek/llm.py`   | `DEEPSEEK_API_KEY`                                 | `https://api.deepseek.com/v1`            |
| `mistral`                   | `mistral/llm.py`    | `MISTRAL_API_KEY`                                  | `https://api.mistral.ai/v1`              |
| `cerebras`                  | `cerebras/llm.py`   | `CEREBRAS_API_KEY`                                 | `https://api.cerebras.ai/v1`             |
| `fireworks`, `fireworks_ai` | `fireworks/llm.py`  | `FIREWORKS_API_KEY`                                | `https://api.fireworks.ai/inference/v1`  |
| `nvidia`                    | `nvidia/llm.py`     | `NVIDIA_API_KEY`, `NV_API_KEY`                     | `https://integrate.api.nvidia.com/v1`    |
| `novita`                    | `novita/llm.py`     | `NOVITA_API_KEY`                                   | `https://api.novita.ai/openai`           |
| `nebius`                    | `nebius/llm.py`     | `NEBIUS_API_KEY`                                   | `https://api.tokenfactory.nebius.com/v1` |
| `sambanova`                 | `sambanova/llm.py`  | `SAMBANOVA_API_KEY`                                | `https://api.sambanova.ai/v1`            |
| `perplexity`                | `perplexity/llm.py` | `PERPLEXITY_API_KEY`                               | `https://api.perplexity.ai/v1`           |
| `qwen`, `dashscope`         | `qwen/llm.py`       | `DASHSCOPE_API_KEY`, `QWEN_API_KEY`                | Aliyun compatible-mode `/v1`             |
| `sarvam`                    | `sarvam/llm.py`     | `SARVAM_API_KEY`                                   | `https://api.sarvam.ai/v1`               |
| `ollama`                    | `ollama/llm.py`     | optional `OLLAMA_API_KEY`; dummy `ollama` if empty | `http://127.0.0.1:11434/v1`              |


**Explicitly not implemented as chat LLM in Go** (fatal or use Python):

- `aws` / `bedrock` — Pipecat `aws/llm.py` (Bedrock SDK adapters).
- `ultravox` — Pipecat `ultravox/llm.py` (Realtime WebSocket API).

Vertex / Google Cloud paths (`google/vertex/llm.py`, etc.) use different clients; use `**LLM=google`** with API keys for the GenAI SDK path implemented here.

### TTS (`TTS=`)


| Env `TTS`              | Pipecat                      | llmpipe                                                                                        |
| ---------------------- | ---------------------------- | ---------------------------------------------------------------------------------------------- |
| `elevenlabs` (default) | `elevenlabs/tts.py`          | `services/elevenlabs`                                                                          |
| `openai`               | `openai/tts.py` (24 kHz PCM) | `services/openai` (`NewTTSService`) — resamples to `SAMPLE_RATE`                               |
| `groq`                 | `groq/tts.py`                | `services/openai` — OpenAI-shaped `/v1/audio/speech` at Groq base (`NewTTSServiceWithBaseURL`) |
| `google`               | `google/tts.py`              | stub                                                                                           |
| `aws`                  | `aws/tts.py`                 | stub                                                                                           |
| `sarvam`               | `sarvam/tts.py`              | `services/sarvam`                                                                              |


Other Pipecat TTS backends (`cartesia`, `deepgram`, `xai`, `kokoro`, `cartesia`, …) are **not** ported; extend `providers.BuildTTS` or use Pipecat Python.

Wiring is centralized in `[../providers/registry.go](../providers/registry.go)`.

## Realtime pipelines (no separate STT/LLM/TTS processors)


| Env `PIPELINE`                | Pipecat                                                                                  | llmpipe                                                                                                             |
| ----------------------------- | ---------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `gemini_live` / `google_live` | `[google/gemini_live/llm.py](../pipecat/src/pipecat/services/google/gemini_live/llm.py)` | `[services/google/gemini_live.go](../services/google/gemini_live.go)` — `genai` `Live.Connect`, `SendRealtimeInput` |
| `openai_realtime`             | `[openai/realtime/llm.py](../pipecat/src/pipecat/services/openai/realtime/llm.py)`       | `[services/openai/realtime.go](../services/openai/realtime.go)` — WebSocket `wss://api.openai.com/v1/realtime`      |


Set `GEMINI_LIVE_MODEL`, `OPENAI_REALTIME_MODEL`, and `OPENAI_API_KEY` as appropriate. Realtime modes **skip VAD** and text aggregators; audio goes straight to the realtime session.

Additional Pipecat realtime modules (`xai/realtime`, `azure/realtime`, `grok/realtime`, `inworld/realtime`, …) are **not** implemented in llmpipe.





