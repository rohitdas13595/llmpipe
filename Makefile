.PHONY: test race bench lint nocgo models onnx-test integration

test:
	go test ./...

race:
	go test -race ./...

bench:
	go test -bench . -benchmem -run '^$$' ./pipeline/...

lint:
	golangci-lint run ./...

# Build and compile tests without cgo. LiveKit and its demo import opus/soxr.
nocgo:
	CGO_ENABLED=0 go build -o /dev/null ./cmd/example-minimal-pipeline/ ./cmd/voicebot/
	CGO_ENABLED=0 go test -count=0 $$(go list ./... | grep -v '/transport/livekit' | grep -v '/cmd/voicebot-livekit')

# Silero VAD v5.1.2 (pinned tag; master has moved on and hashes differently).
SILERO_URL := https://github.com/snakers4/silero-vad/raw/v5.1.2/src/silero_vad/data/silero_vad.onnx
SILERO_SHA := 2623a2953f6ff3d2c1e61740c6cdb7168133479b267dfef114a4a3cc5bdd788f
# Smart Turn v3.2 CPU weights. BSD-2-Clause, not committed (see THIRD_PARTY_NOTICES.md).
SMART_TURN_URL := https://huggingface.co/pipecat-ai/smart-turn-v3/resolve/main/smart-turn-v3.2-cpu.onnx
SMART_TURN_SHA := 2bb026316b14a660486a75b1733cd3fbab8c2fd0314dc9af7be49f8cca967e4f

models:
	./scripts/fetch-model.sh "$(SILERO_URL)" models/silero_vad.onnx "$(SILERO_SHA)"
	./scripts/fetch-model.sh "$(SMART_TURN_URL)" models/smart-turn-v3.2-cpu.onnx "$(SMART_TURN_SHA)"

# Requires ONNXRUNTIME_LIB (libonnxruntime.so.1.22.0) and `make models`.
onnx-test:
	go test -race -tags onnx ./audio/...

integration:
	go test -tags integration ./...
