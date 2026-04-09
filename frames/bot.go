package frames

const KindBotSpeaking = "BotSpeaking"

// BotSpeakingFrame indicates the bot is currently outputting audio.
type BotSpeakingFrame struct{}

func (f *BotSpeakingFrame) FrameKind() string { return KindBotSpeaking }
