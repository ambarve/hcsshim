package cim

import (
	"encoding/binary"
	"path/filepath"
	"syscall"
	"time"
	"unicode/utf16"
)

const (
	// name of the directory in which cims are stored
	cimDir = "cim-layers"

	// 100ns units between Windows NT epoch (Jan 1 1601) and Unix epoch (Jan 1 1970)
	epochDelta = 116444736000000000

	// The name assigned to the vsmb share which shares the cim directory inside the uvm.
	CimVsmbShareName = "bootcimdir"
)

var (
	// Equivalent to SDDL of "D:NO_ACCESS_CONTROL"
	nullSd = []byte{1, 0, 4, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
)

// Filetime is a Windows FILETIME, in 100-ns units since January 1, 1601.
type Filetime int64

// Time returns a Go time equivalent to `ft`.
func (ft Filetime) Time() time.Time {
	if ft == 0 {
		return time.Time{}
	}
	return time.Unix(0, (int64(ft)-epochDelta)*100)
}

func FiletimeFromTime(t time.Time) Filetime {
	if t.IsZero() {
		return 0
	}
	return Filetime(t.UnixNano()/100 + epochDelta)
}

func (ft Filetime) String() string {
	return ft.Time().String()
}

func (ft Filetime) toSyscallFiletime() syscall.Filetime {
	return syscall.NsecToFiletime((int64(ft) - epochDelta) * 100)
}

// A FileInfo specifies information about a file.
type FileInfo struct {
	FileID             uint64 // ignored on write
	Size               int64
	Attributes         uint32
	CreationTime       Filetime
	LastWriteTime      Filetime
	ChangeTime         Filetime
	LastAccessTime     Filetime
	SecurityDescriptor []byte
	ExtendedAttributes []byte
	ReparseData        []byte
}

type OpError struct {
	Cim string
	Op  string
	Err error
}

func (e *OpError) Error() string {
	s := "cim " + e.Op + " " + e.Cim
	s += ": " + e.Err.Error()
	return s
}

// PathError is the error type returned by most functions in this package.
type PathError struct {
	Cim  string
	Op   string
	Path string
	Err  error
}

func (e *PathError) Error() string {
	s := "cim " + e.Op + " " + e.Cim
	s += ":" + e.Path
	s += ": " + e.Err.Error()
	return s
}

type StreamError struct {
	Cim    string
	Op     string
	Path   string
	Stream string
	Err    error
}

func (e *StreamError) Error() string {
	s := "cim " + e.Op + " " + e.Cim
	s += ":" + e.Path
	s += ":" + e.Stream
	s += ": " + e.Err.Error()
	return s
}

type LinkError struct {
	Cim string
	Op  string
	Old string
	New string
	Err error
}

func (e *LinkError) Error() string {
	return "cim " + e.Op + " " + e.Old + " " + e.New + ": " + e.Err.Error()
}

// Usually layers are stored at
// ./root/io.containerd.snapshotter.v1.windows/snapshots/<layerid>. For
// cimfs we must store all layers in the same directory (for forked
// cims to work). So all cim layers are stored in
// /root/io.containerd.snapshotter.v1.windows/snapshots/cim-layers. And
// the cim file representing each individual layer is stored at
// /root/io.containerd.snapshotter.v1.windows/snapshots/cim-layers/<layerid>.cim

// CimName is the filename (<layerid>.cim) of the file representing the cim
func GetCimNameFromLayer(layerPath string) string {
	return filepath.Base(layerPath) + ".cim"
}

// CimPath is the path to the CimDir/<layerid>.cim file that represents a layer cim.
func GetCimPathFromLayer(layerPath string) string {
	layerId := filepath.Base(layerPath)
	dir := filepath.Dir(layerPath)
	return filepath.Join(dir, cimDir, layerId+".cim")
}

// CimDir is the directory inside which all cims are stored.
func GetCimDirFromLayer(layerPath string) string {
	dir := filepath.Dir(layerPath)
	return filepath.Join(dir, cimDir)
}

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
