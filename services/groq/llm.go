// Package groq provides Groq OpenAI-compatible chat (Pipecat: GroqLLMService).
package groq

import (
	"os"

	"github.com/rohitdas13595/llmpipe/aggregate"
	"github.com/rohitdas13595/llmpipe/services"
	"github.com/rohitdas13595/llmpipe/services/openai"
)

// DefaultBaseURL matches Pipecat GroqLLMService.
const DefaultBaseURL = "https://api.groq.com/openai/v1"

// NewLLM returns an OpenAI-compatible client pointed at Groq.
func NewLLM(name string, c *aggregate.LLMContext, re services.ReenterFunc) *openai.LLM {
	key := os.Getenv("GROQ_API_KEY")
	model := os.Getenv("GROQ_MODEL")
	if model == "" {
		model = "llama-3.3-70b-versatile"
	}
	return openai.NewLLMWithBaseURL(name, key, DefaultBaseURL, model, c, re)
}
