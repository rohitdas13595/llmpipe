package frames

// OutputTransportMessageFrame is a non-urgent message for the output
// transport to deliver to the client (JSON text, for example).
type OutputTransportMessageFrame struct {
	Data string
	Meta
}

func (f *OutputTransportMessageFrame) FrameKind() string { return KindOutputTransportMessage }

// OutputTransportMessageUrgentFrame is the same payload as
// OutputTransportMessageFrame but system priority, so it is not stuck behind
// queued audio. Barge-in uses it to carry TransportClearPayload.
type OutputTransportMessageUrgentFrame struct {
	Data string
	System
	Meta
}

func (f *OutputTransportMessageUrgentFrame) FrameKind() string {
	return KindOutputTransportUrgent
}
