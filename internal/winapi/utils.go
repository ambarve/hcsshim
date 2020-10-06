package winapi

import (
	"encoding/binary"
	"errors"
	"strings"
	"syscall"
	"unicode/utf16"
	"unsafe"
)

type UnicodeString struct {
	Length        uint16
	MaximumLength uint16
	Buffer        *uint16
}

// parseUtf16LE parses a UTF-16LE byte array into a string (without passing
// through a uint16 or rune array).
func ParseUtf16LE(b []byte) string {
	var sb strings.Builder
	sb.Grow(len(b) / 2)
	for len(b) > 0 {
		r := rune(binary.LittleEndian.Uint16(b))
		if utf16.IsSurrogate(r) && len(b) > 2 {
			sb.WriteRune(utf16.DecodeRune(r, rune(binary.LittleEndian.Uint16(b[2:]))))
			b = b[4:]
		} else {
			sb.WriteRune(r)
			b = b[2:]
		}
	}
	return sb.String()
}

//String converts a UnicodeString to a golang string
func (uni UnicodeString) String() string {
	p := (*[0xffff]uint16)(unsafe.Pointer(uni.Buffer))

	// UnicodeString is not guaranteed to be null terminated, therefore
	// use the UnicodeString's Length field
	lengthInChars := uni.Length / 2
	return syscall.UTF16ToString(p[:lengthInChars])
}

// NewUnicodeString allocates a new UnicodeString and copies `s` into
// the buffer of the new UnicodeString.
func NewUnicodeString(s string) (*UnicodeString, error) {
	ws := utf16.Encode(([]rune)(s))
	if len(ws) > 32767 {
		return nil, syscall.ENAMETOOLONG
	}

	uni := &UnicodeString{
		Length:        uint16(len(ws) * 2),
		MaximumLength: uint16(len(ws) * 2),
		Buffer:        &make([]uint16, len(ws))[0],
	}
	copy((*[32768]uint16)(unsafe.Pointer(uni.Buffer))[:], ws)
	return uni, nil
}

// ConvertStringSetToSlice is a helper function used to convert the contents of
// `buf` into a string slice. `buf` contains a set of null terminated strings
// with an additional null at the end to indicate the end of the set.
func ConvertStringSetToSlice(buf []byte) ([]string, error) {
	var results []string
	prev := 0
	for i := range buf {
		if buf[i] == 0 {
			if prev == i {
				// found two null characters in a row, return result
				return results, nil
			}
			results = append(results, string(buf[prev:i]))
			prev = i + 1
		}
	}
	return nil, errors.New("string set malformed: missing null terminator at end of buffer")
}
