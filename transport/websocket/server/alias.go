// Package server is the Pipecat "websocket server" analogue: PCM WebSocket in/out for browsers and demo clients.
// The implementation lives in transport/ws; this package provides stable import paths alongside Pipecat.
package server

import "github.com/rohitdas13595/llmpipe/transport/ws"

// Transport is the Gorilla WebSocket PCM transport.
type Transport = ws.Transport

// NewTransport mirrors [ws.NewTransport].
var NewTransport = ws.NewTransport

// Upgrader is the default HTTP → WebSocket upgrader (permissive CheckOrigin for local dev).
var Upgrader = ws.Upgrader
