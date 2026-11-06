package providers

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/rohitdas13595/llmpipe/processor"
	"github.com/rohitdas13595/llmpipe/processors/tools"
)

// ToolRegistryFromEnv registers the demo tools selected by TOOLS.
// TOOLS=weather adds get_weather, which returns a canned forecast so the
// voice demo can exercise function calling without a weather API key.
// An empty TOOLS value returns nil and leaves the pipeline unchanged.
func ToolRegistryFromEnv() *tools.Registry {
	enabled := map[string]bool{}
	for _, part := range strings.Split(os.Getenv("TOOLS"), ",") {
		part = strings.ToLower(strings.TrimSpace(part))
		if part != "" {
			enabled[part] = true
		}
	}
	if len(enabled) == 0 {
		return nil
	}
	reg := tools.NewRegistry()
	if enabled["weather"] {
		fn, h := tools.Typed("get_weather", "Get the current weather for a location.", func(ctx context.Context, a weatherArgs) (any, error) {
			if strings.TrimSpace(a.Location) == "" {
				return nil, errLocation
			}
			return map[string]any{
				"location":      a.Location,
				"temperature_c": 21,
				"condition":     "partly cloudy",
			}, nil
		})
		reg.Register(fn, h)
		log.Printf("tools: get_weather (TOOLS=weather)")
	}
	if len(reg.Schema().Functions) == 0 {
		return nil
	}
	return reg
}

type weatherArgs struct {
	Location string `json:"location" jsonschema:"description=City or place name"`
}

type locationError string

func (e locationError) Error() string { return string(e) }

const errLocation locationError = "location is required"

// WithToolPrompt reminds the model to call the demo tools when they are enabled.
func WithToolPrompt(base string) string {
	if !strings.Contains(strings.ToLower(os.Getenv("TOOLS")), "weather") {
		return base
	}
	return strings.TrimSpace(base + " When the user asks about the weather, call get_weather.")
}

// AttachTools gives a processor the demo registry when it accepts one.
func AttachTools(p processor.Processor, r *tools.Registry) {
	if p == nil || r == nil {
		return
	}
	if s, ok := p.(interface{ SetRegistry(*tools.Registry) }); ok {
		s.SetRegistry(r)
	}
}
