// Package adapters converts typed context and tool schemas into provider payloads.
package adapters

import (
	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/processors/tools"
)

// Adapter turns the shared transcript and tool schema into one provider's wire format.
type Adapter interface {
	Messages([]aggregate.Message) (any, error)
	Tools(tools.Schema) (any, error)
}
