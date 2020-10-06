package cim

import (
	"encoding/binary"
	"unicode/utf16"
)

// cmpcaseUtf8Utf16LE compares a UTF-8 string with a UTF-16LE encoded byte
// array, upcasing each rune through the upcase table.
func cmpcaseUtf8Utf16LE(a string, b []byte, upcase []uint16) int {
	for _, ar := range a {
		if len(b) == 0 {
			return 1
		}
		if int(ar) < len(upcase) {
			ar = rune(upcase[int(ar)])
		}
		br := rune(binary.LittleEndian.Uint16(b))
		bs := 2
		if utf16.IsSurrogate(br) {
			if len(b) == bs {
				return 1 // error?
			}
			br = utf16.DecodeRune(br, rune(binary.LittleEndian.Uint16(b[bs:])))
			if br == '\ufffd' {
				return 1 // error?
			}
			bs += 2
		} else {
			br = rune(upcase[int(br)])
		}
		if ar < br {
			return -1
		} else if ar > br {
			return 1
		}
		b = b[bs:]
	}
	if len(b) > 0 {
		return -1
	}
	return 0
}
