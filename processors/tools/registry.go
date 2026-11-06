package tools

import "sync"

// Registry is the process-wide set of functions a pipeline can call.
type Registry struct {
	mu    sync.Mutex
	order []string
	by    map[string]registered
}

type registered struct {
	fn  Function
	h   Handler
	opt Options
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{by: map[string]registered{}}
}

// Register adds or replaces fn. See Options for default timeout and cancellation.
func (r *Registry) Register(fn Function, h Handler, o ...Options) {
	if r == nil || fn.Name == "" || h == nil {
		return
	}
	opt := Options{CancelOnInterruption: true, Timeout: DefaultTimeout}
	if len(o) > 0 {
		opt = o[0]
		switch {
		case opt.Timeout < 0:
			opt.Timeout = 0
		case opt.Timeout == 0:
			opt.Timeout = DefaultTimeout
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.by[fn.Name]; !ok {
		r.order = append(r.order, fn.Name)
	}
	r.by[fn.Name] = registered{fn: fn, h: h, opt: opt}
}

// Schema returns a copy of the registered declarations, in registration order.
func (r *Registry) Schema() Schema {
	if r == nil {
		return Schema{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Function, 0, len(r.order))
	for _, name := range r.order {
		e, ok := r.by[name]
		if !ok {
			continue
		}
		fn := e.fn
		if fn.Parameters != nil {
			cp := make(map[string]any, len(fn.Parameters))
			for k, v := range fn.Parameters {
				cp[k] = v
			}
			fn.Parameters = cp
		}
		out = append(out, fn)
	}
	return Schema{Functions: out}
}

// Lookup returns the handler registered under name.
func (r *Registry) Lookup(name string) (Function, Handler, Options, bool) {
	if r == nil {
		return Function{}, nil, Options{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.by[name]
	if !ok {
		return Function{}, nil, Options{}, false
	}
	return e.fn, e.h, e.opt, true
}
