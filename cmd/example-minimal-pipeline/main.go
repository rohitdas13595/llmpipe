// Command example-minimal-pipeline shows a tiny llmpipe chain without network providers:
// a passthrough-style processor forwards a TextFrame; useful as a template for custom stages.
//
// Run from the llmpipe module: go run ./cmd/example-minimal-pipeline
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/rohitdas13595/llmpipe/frames"
	"github.com/rohitdas13595/llmpipe/observe"
	"github.com/rohitdas13595/llmpipe/pipeline"
	"github.com/rohitdas13595/llmpipe/processor"
)

func main() {
	ctx := context.Background()

	p := pipeline.NewPipeline(
		processor.Func{
			N: "echo",
			F: func(ctx context.Context, f frames.Frame, dir processor.Direction, emit processor.Emit) error {
				if t, ok := f.(*frames.TextFrame); ok {
					fmt.Printf("processor sees: %q\n", t.Text)
				}
				emit.Down(f)
				return nil
			},
		},
	)

	var last string
	task := pipeline.NewPipelineTask(p, pipeline.WithObservers(observe.FuncObserver{F: func(pushed observe.FramePushed) {
		if t, ok := pushed.Frame.(*frames.TextFrame); ok {
			last = t.Text
		}
	}}))

	if err := task.QueueFrames(ctx, []frames.Frame{&frames.TextFrame{Text: "hello, llmpipe"}}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("observer last text:", last)
}
