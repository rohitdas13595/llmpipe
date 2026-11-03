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

# Phase 4 downloads Silero / Smart Turn weights. Kept so the CI target exists.
models:
	@echo "onnx models are downloaded in phase 4"

onnx-test:
	@echo "onnx tests arrive in phase 4: go test -race -tags onnx ./audio/..."

integration:
	go test -tags integration ./...
