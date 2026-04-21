// Package audiofmt builds minimal WAV containers for APIs that expect audio/wav.
package audiofmt

import (
	"bytes"
	"encoding/binary"
)

// MonoS16LE builds a WAV file (headers + PCM) for mono 16-bit little-endian samples.
func MonoS16LE(pcm []byte, sampleRate int) []byte {
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	n := len(pcm)
	blockAlign := uint16(2)
	byteRate := uint32(sampleRate) * uint32(blockAlign)
	subchunk2Size := uint32(n)
	chunkSize := uint32(36) + subchunk2Size

	var b bytes.Buffer
	b.Write([]byte("RIFF"))
	_ = binary.Write(&b, binary.LittleEndian, chunkSize)
	b.Write([]byte("WAVE"))
	b.Write([]byte("fmt "))
	_ = binary.Write(&b, binary.LittleEndian, uint32(16)) // subchunk1Size
	_ = binary.Write(&b, binary.LittleEndian, uint16(1))  // PCM
	_ = binary.Write(&b, binary.LittleEndian, uint16(1))  // channels
	_ = binary.Write(&b, binary.LittleEndian, uint32(sampleRate))
	_ = binary.Write(&b, binary.LittleEndian, byteRate)
	_ = binary.Write(&b, binary.LittleEndian, blockAlign)
	_ = binary.Write(&b, binary.LittleEndian, uint16(16)) // bits per sample
	b.Write([]byte("data"))
	_ = binary.Write(&b, binary.LittleEndian, subchunk2Size)
	b.Write(pcm)
	return b.Bytes()
}
