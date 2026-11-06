package frames

import (
	"sync/atomic"
	"time"
)

// Meta is routing metadata assigned lazily the first time a frame is queued.
// Embed it at the end of a frame struct so existing keyed composite literals
// keep compiling. ID is unique per assignment; PTS is reserved for playback
// timestamps (phase 2) and stays zero until a producer sets it.
type Meta struct {
	ID        uint64
	CreatedAt time.Time
	PTS       time.Duration
}

// FrameMeta returns the embedded metadata. Promoted onto every frame pointer
// that embeds Meta.
func (m *Meta) FrameMeta() *Meta { return m }

var nextMetaID atomic.Uint64

// EnsureMeta assigns ID and CreatedAt when ID is still zero.
// Call it before the frame is shared with another goroutine.
func EnsureMeta(f Frame) {
	if f == nil {
		return
	}
	c, ok := f.(interface{ FrameMeta() *Meta })
	if !ok {
		return
	}
	m := c.FrameMeta()
	if m == nil || atomic.LoadUint64(&m.ID) != 0 {
		return
	}
	id := nextMetaID.Add(1)
	if atomic.CompareAndSwapUint64(&m.ID, 0, id) && m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now()
	}
}

// MetaID returns the assigned ID, or 0 when f has no metadata yet.
func MetaID(f Frame) uint64 {
	if f == nil {
		return 0
	}
	c, ok := f.(interface{ FrameMeta() *Meta })
	if !ok || c.FrameMeta() == nil {
		return 0
	}
	return atomic.LoadUint64(&c.FrameMeta().ID)
}
