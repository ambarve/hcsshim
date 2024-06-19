//go:build windows

package winapi

import (
	"unsafe"

	"github.com/Microsoft/go-winio/pkg/guid"
	"golang.org/x/sys/windows"
)

type g = guid.GUID
type FsHandle uintptr
type StreamHandle uintptr

type CimFsFileMetadata struct {
	Attributes uint32
	FileSize   int64

	CreationTime   windows.Filetime
	LastWriteTime  windows.Filetime
	ChangeTime     windows.Filetime
	LastAccessTime windows.Filetime

	SecurityDescriptorBuffer unsafe.Pointer
	SecurityDescriptorSize   uint32

	ReparseDataBuffer unsafe.Pointer
	ReparseDataSize   uint32

	ExtendedAttributes unsafe.Pointer
	EACount            uint32
}

type CimFsImagePath struct {
	ImageDir  *uint16
	ImageName *uint16
}

// TODO(ambarve): This should ideally be added to go-winio
// defined here: https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/ns-ntifs-file_stat_basic_information
type FileStatBasicInformation struct {
	FileID                int64
	CreationTime          windows.Filetime
	LastAccessTime        windows.Filetime
	LastWriteTime         windows.Filetime
	ChangeTime            windows.Filetime
	AllocationSize        int64
	EndOfFile             int64
	FileAttributes        uint32
	ReparseTag            uint32
	NumberOfLinks         uint32
	DeviceType            uint32
	DeviceCharacteristics uint32
	Reserved              uint32
	VolumeSerialNumber    int64
	FileId128             [16]byte
}

//sys CimMountImage(imagePath string, fsName string, flags uint32, volumeID *g) (hr error) = cimfs.CimMountImage?
//sys CimDismountImage(volumeID *g) (hr error) = cimfs.CimDismountImage?

//sys CimCreateImage(imagePath string, oldFSName *uint16, newFSName *uint16, cimFSHandle *FsHandle) (hr error) = cimfs.CimCreateImage?
//sys CimCreateImage2(imagePath string, flags uint32, oldFSName *uint16, newFSName *uint16, cimFSHandle *FsHandle) (hr error) = cimfs.CimCreateImage2?
//sys CimCloseImage(cimFSHandle FsHandle) = cimfs.CimCloseImage
//sys CimCommitImage(cimFSHandle FsHandle) (hr error) = cimfs.CimCommitImage?

//sys CimCreateFile(cimFSHandle FsHandle, path string, file *CimFsFileMetadata, cimStreamHandle *StreamHandle) (hr error) = cimfs.CimCreateFile?
//sys CimCloseStream(cimStreamHandle StreamHandle) (hr error) = cimfs.CimCloseStream?
//sys CimWriteStream(cimStreamHandle StreamHandle, buffer uintptr, bufferSize uint32) (hr error) = cimfs.CimWriteStream?
//sys CimDeletePath(cimFSHandle FsHandle, path string) (hr error) = cimfs.CimDeletePath?
//sys CimCreateHardLink(cimFSHandle FsHandle, newPath string, oldPath string) (hr error) = cimfs.CimCreateHardLink?
//sys CimCreateAlternateStream(cimFSHandle FsHandle, path string, size uint64, cimStreamHandle *StreamHandle) (hr error) = cimfs.CimCreateAlternateStream?
//sys CimAddFsToMergedImage(cimFSHandle FsHandle, path string) (hr error) = cimfs.CimAddFsToMergedImage?
//sys CimAddFsToMergedImage2(cimFSHandle FsHandle, path string, flags uint32) (hr error) = cimfs.CimAddFsToMergedImage2?
//sys CimMergeMountImage(numCimPaths uint32, backingImagePaths *CimFsImagePath, flags uint32, volumeID *g) (hr error) = cimfs.CimMergeMountImage?
//sys CimReadFile2(imagePath string, filePath string, offset uint64, buffer unsafe.Pointer, bufferSize uint64, bytesRead *uint64, bytesRemaining *uint64, flags uint32) (hr error) = cimfs.CimReadFile2?
//sys CimGetFileStatBasicInformation2(imagePath string, filePath string, info *FileStatBasicInformation, flags uint32) (hr error) = cimfs.CimGetFileStatBasicInformation2?
