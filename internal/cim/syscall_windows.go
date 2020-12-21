package cim

import (
	"syscall"
	"unsafe"

	"github.com/Microsoft/go-winio/pkg/guid"
)

type g = guid.GUID
type orHKey uintptr
type FsHandle uintptr
type StreamHandle uintptr

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

//sys cimCreateImage(imagePath string, oldFSName *uint16, newFSName *uint16, cimFSHandle *FsHandle) (hr error) = cimfs.CimCreateImage
//sys cimCloseImage(cimFSHandle FsHandle) (hr error) = cimfs.CimCloseImage
//sys cimCommitImage(cimFSHandle FsHandle) (hr error) = cimfs.CimCommitImage

//sys cimCreateFile(cimFSHandle FsHandle, path string, file *cimFsFileMetadata, cimStreamHandle *StreamHandle) (hr error) = cimfs.CimCreateFile
//sys cimCloseStream(cimStreamHandle StreamHandle) (hr error) = cimfs.CimCloseStream
//sys cimWriteStream(cimStreamHandle StreamHandle, buffer uintptr, bufferSize uint32) (hr error) = cimfs.CimWriteStream
//sys cimDeletePath(cimFSHandle FsHandle, path string) (hr error) = cimfs.CimDeletePath
//sys cimCreateHardLink(cimFSHandle FsHandle, newPath string, oldPath string) (hr error) = cimfs.CimCreateHardLink
//sys cimCreateAlternateStream(cimFSHandle FsHandle, path string, size uint64, cimStreamHandle *StreamHandle) (hr error) = cimfs.CimCreateAlternateStream
