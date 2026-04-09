// Package pipeline provides Pipeline creation, PipelineTask, and PipelineRunner.
package pipeline

import (
	"github.com/rohitdas13595/llmpipe/processor"
)

// Pipeline is a linear chain of processors.
type Pipeline struct {
	processors []processor.Processor
}

// NewPipeline builds a pipeline from an ordered slice and links are logical
// (execution order is handled by PipelineTask.processAt).
func NewPipeline(processors ...processor.Processor) *Pipeline {
	ps := make([]processor.Processor, len(processors))
	copy(ps, processors)
	return &Pipeline{processors: ps}
}

// Processors returns the processor chain (for task construction).
func (p *Pipeline) Processors() []processor.Processor {
	return p.processors
}
