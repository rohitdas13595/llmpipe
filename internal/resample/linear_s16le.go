package resample

import "encoding/binary"

// LinearS16LE resamples mono 16-bit LE PCM from fromRate to toRate (Hz).
func LinearS16LE(pcm []byte, fromRate, toRate int) []byte {
	if len(pcm) < 2 || fromRate <= 0 || toRate <= 0 || fromRate == toRate {
		return append([]byte(nil), pcm...)
	}
	nIn := len(pcm) / 2
	// output duration same → nOut samples = nIn * toRate / fromRate
	nOut := nIn * toRate / fromRate
	if nOut < 1 {
		nOut = 1
	}
	out := make([]byte, nOut*2)
	for i := 0; i < nOut; i++ {
		srcF := float64(i) * float64(fromRate) / float64(toRate)
		i0 := int(srcF)
		frac := srcF - float64(i0)
		if i0 >= nIn-1 {
			i0 = nIn - 2
			if i0 < 0 {
				i0 = 0
				frac = 0
			}
		}
		s0 := int16(binary.LittleEndian.Uint16(pcm[2*i0 : 2*i0+2]))
		s1 := int16(binary.LittleEndian.Uint16(pcm[2*i0+2 : 2*i0+4]))
		v := float64(s0)*(1-frac) + float64(s1)*frac
		s := int16(v)
		out[2*i] = byte(s)
		out[2*i+1] = byte(s >> 8)
	}
	return out
}
