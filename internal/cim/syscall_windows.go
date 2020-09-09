package cim

import (
	"syscall"
	"unsafe"

	"github.com/Microsoft/go-winio/pkg/guid"
)

type g = guid.GUID
type orHKey uintptr

type cimFsFileMetadata struct {
	Attributes uint32
	FileSize   int64

	CreationTime   syscall.Filetime
	LastWriteTime  syscall.Filetime
	ChangeTime     syscall.Filetime
	LastAccessTime syscall.Filetime

	SecurityDescriptorBuffer unsafe.Pointer
	SecurityDescriptorSize   uint32

	ReparseDataBuffer unsafe.Pointer
	ReparseDataSize   uint32

	ExtendedAttributes unsafe.Pointer
	EACount            uint32
}

//go:generate go run ../../mksyscall_windows.go -output zsyscall_windows.go syscall_windows.go

//sys cimMountImage(imagePath string, fsName string, flags uint32, volumeID *g) (hr error) = cimfs.CimMountImage
//sys cimDismountImage(volumeID *g) (hr error) = cimfs.CimDismountImage

//sys cimCreateImage(imagePath string, oldFSName *uint16, newFSName *uint16, cimFSHandle *fsHandle) (hr error) = cimfs.CimCreateImage
//sys cimCloseImage(cimFSHandle fsHandle) (hr error) = cimfs.CimCloseImage
//sys cimCommitImage(cimFSHandle fsHandle) (hr error) = cimfs.CimCommitImage

//sys cimCreateFile(cimFSHandle fsHandle, path string, file *cimFsFileMetadata, cimStreamHandle *streamHandle) (hr error) = cimfs.CimCreateFile
//sys cimCloseStream(cimStreamHandle streamHandle) (hr error) = cimfs.CimCloseStream
//sys cimWriteStream(cimStreamHandle streamHandle, buffer uintptr, bufferSize uint32) (hr error) = cimfs.CimWriteStream
//sys cimDeletePath(cimFSHandle fsHandle, path string) (hr error) = cimfs.CimDeletePath
//sys cimCreateHardLink(cimFSHandle fsHandle, newPath string, oldPath string) (hr error) = cimfs.CimCreateHardLink
//sys cimCreateAlternateStream(cimFSHandle fsHandle, path string, size uint64, cimStreamHandle *streamHandle) (hr error) = cimfs.CimCreateAlternateStream
