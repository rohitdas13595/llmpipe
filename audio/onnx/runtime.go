//go:build onnx

package onnx

import (
	"fmt"
	"os"
	"sync"

	ort "github.com/yalue/onnxruntime_go"
)

// ORTVersion is the onnxruntime release this binding was built against.
const ORTVersion = "1.22.0"

// Linked reports that this binary includes ONNX Runtime.
func Linked() bool { return true }

var (
	once    sync.Once
	initErr error
)

// Init loads the shared library from ONNXRUNTIME_LIB and initialises the
// process-wide runtime. Later calls return the first result.
func Init() error {
	once.Do(func() {
		lib := os.Getenv("ONNXRUNTIME_LIB")
		if lib == "" {
			initErr = fmt.Errorf("onnx: ONNXRUNTIME_LIB is unset; point it at libonnxruntime.so.%s and rebuild with -tags onnx", ORTVersion)
			return
		}
		// The operator names the runtime library; it is not request data.
		if _, err := os.Stat(lib); err != nil { //nolint:gosec // G304: operator-supplied library path
			initErr = fmt.Errorf("onnx: ONNXRUNTIME_LIB %s: %w", lib, err)
			return
		}
		ort.SetSharedLibraryPath(lib)
		if err := ort.InitializeEnvironment(); err != nil {
			initErr = fmt.Errorf("onnx: initialise runtime: %w", err)
		}
	})
	return initErr
}

// Info is one model input or output.
type Info struct {
	Name string
}

// Session is one loaded model. Run is safe for one caller at a time.
type Session struct {
	raw     *ort.DynamicAdvancedSession
	Inputs  []Info
	Outputs []Info
}

// NewSession opens path. inputs and outputs are the tensor names in the
// order Run will use. threads is the intra-op pool size; values below 1
// become 1.
func NewSession(path string, threads int, inputs, outputs []string) (*Session, error) {
	if err := Init(); err != nil {
		return nil, err
	}
	if threads < 1 {
		threads = 1
	}
	opts, err := ort.NewSessionOptions()
	if err != nil {
		return nil, fmt.Errorf("onnx: session options: %w", err)
	}
	defer func() { _ = opts.Destroy() }()
	if err := opts.SetIntraOpNumThreads(threads); err != nil {
		return nil, err
	}
	if err := opts.SetInterOpNumThreads(1); err != nil {
		return nil, err
	}
	if err := opts.SetGraphOptimizationLevel(ort.GraphOptimizationLevelEnableAll); err != nil {
		return nil, err
	}
	raw, err := ort.NewDynamicAdvancedSession(path, inputs, outputs, opts)
	if err != nil {
		return nil, fmt.Errorf("onnx: load %s: %w", path, err)
	}
	s := &Session{raw: raw}
	for _, n := range inputs {
		s.Inputs = append(s.Inputs, Info{Name: n})
	}
	for _, n := range outputs {
		s.Outputs = append(s.Outputs, Info{Name: n})
	}
	return s, nil
}

// Run executes the session. outputs may be nil entries, which the runtime
// allocates; the caller destroys every returned output.
func (s *Session) Run(inputs, outputs []ort.Value) error {
	if s == nil || s.raw == nil {
		return fmt.Errorf("onnx: session is closed")
	}
	return s.raw.Run(inputs, outputs)
}

// Close releases the session. The process-wide runtime stays initialised.
func (s *Session) Close() error {
	if s == nil || s.raw == nil {
		return nil
	}
	err := s.raw.Destroy()
	s.raw = nil
	return err
}
