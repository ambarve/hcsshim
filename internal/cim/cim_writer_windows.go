package cim

import (
	"errors"
	"path/filepath"
	"unsafe"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

type cimfsFileMetadata struct {
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

type fsHandle uintptr
type streamHandle uintptr

// cim represents a single CimFS filesystem instance. On disk, the image is
// composed of a filesystem file and several object ID and region files.
type cim struct {
	name string
	// handle is the CIMFS_IMAGE_HANDLE that must be passed when calling CIMFS APIs.
	handle fsHandle
	// name of the active file
	activeName string
	// stream to currently active file
	activeStream streamHandle
	// amount of bytes that can be written to the activeStream
	activeLeft int64
}

// creates a new cim image. The handle returned in the `cim.handle` variable can then
// be used to do operations on this cim.
func create(imagePath string, oldFSName string, newFSName string) (_ *cim, err error) {
	var oldNameBytes *uint16
	if oldFSName != "" {
		oldNameBytes, err = windows.UTF16PtrFromString(oldFSName)
		if err != nil {
			return nil, err
		}
	}
	var newNameBytes *uint16
	if newFSName != "" {
		newNameBytes, err = windows.UTF16PtrFromString(newFSName)
		if err != nil {
			return nil, err
		}
	}
	var handle fsHandle
	if err := cimCreateImage(imagePath, oldNameBytes, newNameBytes, &handle); err != nil {
		return nil, err
	}
	return &cim{handle: handle, name: filepath.Join(imagePath, newFSName)}, nil
}

// creates alternate stream of given size at the given path
// relative to the cim path. This will replace the current active
// stream. Always, finish writing current active stream and then
// create an alternate stream.
func (c *cim) createAlternateStream(path string, size uint64) (err error) {
	err = c.closeStream()
	if err != nil {
		return err
	}
	err = cimCreateAlternateStream(c.handle, path, size, &c.activeStream)
	if err != nil {
		return err
	}
	return nil
}

// closes the currently active stream
func (c *cim) closeStream() error {
	if c.activeStream == 0 {
		return nil
	}
	err := cimCloseStream(c.activeStream)
	if err == nil && c.activeLeft > 0 {
		// Validate here because CimCloseStream does not and this improves error
		// reporting. Otherwise the error will occur in the context of
		// cimWriteStream.
		err = errors.New("write truncated")
	}
	if err != nil {
		err = &PathError{Cim: c.name, Op: "closeStream", Path: c.activeName, Err: err}
	}
	c.activeLeft = 0
	c.activeStream = 0
	c.activeName = ""
	return err
}

// addFile adds a new file to the image. The file is added at the
// specified path. After calling this function, the file is set as the active
// stream for the image, so data can be written by calling `Write`.
func (c *cim) addFile(path string, info winio.FileBasicInfo, fileSize int64, securityDescriptor []byte, extendedAttributes []byte, reparseData []byte) error {
	err := c.closeStream()
	if err != nil {
		return err
	}
	fileMetadata := &cimfsFileMetadata{
		Attributes:     info.FileAttributes,
		FileSize:       fileSize,
		CreationTime:   toWindowsTimeFormat(info.CreationTime),
		LastWriteTime:  toWindowsTimeFormat(info.LastWriteTime),
		ChangeTime:     toWindowsTimeFormat(info.ChangeTime),
		LastAccessTime: toWindowsTimeFormat(info.LastAccessTime),
	}
	sd := securityDescriptor
	if len(sd) == 0 {
		// Passing an empty security descriptor creates a CIM in a weird state.
		// Pass the NULL DACL.
		sd = nullSd
	}
	fileMetadata.SecurityDescriptorBuffer = unsafe.Pointer(&sd[0])
	fileMetadata.SecurityDescriptorSize = uint32(len(sd))
	if len(reparseData) > 0 {
		fileMetadata.ReparseDataBuffer = unsafe.Pointer(&reparseData[0])
		fileMetadata.ReparseDataSize = uint32(len(reparseData))
	}
	if len(extendedAttributes) > 0 {
		fileMetadata.ExtendedAttributes = unsafe.Pointer(&extendedAttributes[0])
		fileMetadata.EACount = uint32(len(extendedAttributes))
	}
	err = cimCreateFile(c.handle, path, fileMetadata, &c.activeStream)
	if err != nil {
		return &PathError{Cim: c.name, Op: "addFile", Path: path, Err: err}
	}
	c.activeName = path
	if info.FileAttributes&(FILE_ATTRIBUTE_DIRECTORY|FILE_ATTRIBUTE_SPARSE_FILE) == 0 {
		c.activeLeft = fileSize
	}
	return nil
}

// write writes bytes to the active stream.
func (c *cim) write(p []byte) (int, error) {
	if c.activeStream == 0 {
		return 0, errors.New("no active stream")
	}
	if int64(len(p)) > c.activeLeft {
		return 0, &PathError{Cim: c.name, Op: "write", Path: c.activeName, Err: errors.New("wrote too much")}
	}
	err := cimWriteStream(c.activeStream, uintptr(unsafe.Pointer(&p[0])), uint32(len(p)))
	if err != nil {
		err = &PathError{Cim: c.name, Op: "write", Path: c.activeName, Err: err}
		return 0, err
	}
	c.activeLeft -= int64(len(p))
	return len(p), nil
}

// Link adds a hard link from `oldPath` to `newPath` in the image.
// TODO(ambarve): there is a bug in cimfs hard link implementation that it will always
// show the number of hard links to be 1 even if there are more.
func (c *cim) addLink(oldPath string, newPath string) error {
	err := c.closeStream()
	if err != nil {
		return err
	}
	err = cimCreateHardLink(c.handle, newPath, oldPath)
	if err != nil {
		err = &LinkError{Cim: c.name, Op: "addLink", Old: oldPath, New: newPath, Err: err}
	}
	return err
}

// unlink deletes the file at `path` from the image.
func (c *cim) unlink(path string) error {
	err := c.closeStream()
	if err != nil {
		return err
	}
	err = cimDeletePath(c.handle, path)
	if err != nil {
		err = &PathError{Cim: c.name, Op: "unlink", Path: path, Err: err}
	}
	return err
}

func (c *cim) commit() error {
	err := c.closeStream()
	if err != nil {
		return err
	}
	err = cimCommitImage(c.handle)
	if err != nil {
		err = &OpError{Cim: c.name, Op: "commit", Err: err}
	}
	return err
}

// close closes the CimFS filesystem.
func (c *cim) close() error {
	if c.handle == 0 {
		return errors.New("invalid writer")
	}
	if err := c.commit(); err != nil {
		return &OpError{Cim: c.name, Op: "commit", Err: err}
	}
	if err := cimCloseImage(c.handle); err != nil {
		return &OpError{Cim: c.name, Op: "close", Err: err}
	}
	c.handle = 0
	return nil
}
