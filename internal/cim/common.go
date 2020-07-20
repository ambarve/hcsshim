package cim

import (
	"fmt"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Microsoft/go-winio/pkg/guid"
	"github.com/Microsoft/hcsshim/internal/mylogger"
	"golang.org/x/sys/windows"
)

const (
	// Windows file attributes.
	FILE_ATTRIBUTE_READONLY      = 0x00000001
	FILE_ATTRIBUTE_HIDDEN        = 0x00000002
	FILE_ATTRIBUTE_SYSTEM        = 0x00000004
	FILE_ATTRIBUTE_DIRECTORY     = 0x00000010
	FILE_ATTRIBUTE_ARCHIVE       = 0x00000020
	FILE_ATTRIBUTE_SPARSE_FILE   = 0x00000200
	FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400

	// name of the directory in which cims are stored
	cimDir = "cim-layers"

	// 100ns units between Windows NT epoch (Jan 1 1601) and Unix epoch (Jan 1 1970)
	epochDelta = 116444736000000000
)

var (
	// Equivalent to SDDL of "D:NO_ACCESS_CONTROL"
	nullSd = []byte{1, 0, 4, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	// map for GUIDs of cims mounted by this shim.
	cimGuidMap = make(map[string]guid.GUID)
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

// Todo(ambarve): Figure this out later
// func toWindowsTimeFormat(ft syscall.Filetime) windows.Filetime {
// 	return windows.Filetime{
// 		LowDateTime:  uint32(ft.LowDateTime),
// 		HighDateTime: uint32(ft.HighDateTime >> 32),
// 	}
// }

func convertFileTime(ft syscall.Filetime) windows.Filetime {
	return windows.Filetime{
		LowDateTime:  ft.LowDateTime,
		HighDateTime: ft.HighDateTime,
	}
}

func toNtPath(p string) string {
	p = filepath.FromSlash(p)
	p = strings.ToLower(p)
	for len(p) > 0 && p[0] == filepath.Separator {
		p = p[1:]
	}
	return p
}

// Usually layers are stored at
// ./root/io.containerd.snapshotter.v1.windows/snapshots/<layerid>. For
// cimfs we must store all layers in the same directory (for forked
// cims to work). So all cim layers are stored in
// /root/io.containerd.snapshotter.v1.windows/snapshots/cim-layers. And
// the cim file representing each individual layer is stored at
// /root/io.containerd.snapshotter.v1.windows/snapshots/cim-layers/<layerid>.cim

func GetCimName(layerPath string) string {
	return filepath.Base(layerPath) + ".cim"
}

func GetCimPath(layerPath string) string {
	layerId := filepath.Base(layerPath)
	dir := filepath.Dir(layerPath)
	return filepath.Join(dir, cimDir, layerId+".cim")
}

func GetCimDir(layerPath string) string {
	dir := filepath.Dir(layerPath)
	return filepath.Join(dir, cimDir)
}

func AssignGUIDToCim(cimPath string) (guid.GUID, error) {
	if _, ok := cimGuidMap[cimPath]; !ok {
		layerGUID, err := guid.NewV4()
		mylogger.LogFmt("assigning new guid: %v, to path: %v\n", layerGUID, cimPath)
		if err != nil {
			return layerGUID, fmt.Errorf("error creating guid: %s", err)
		}
		cimGuidMap[cimPath] = layerGUID
	}
	return cimGuidMap[cimPath], nil
}
