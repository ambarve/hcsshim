package cim

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

type fsHandle uintptr
type streamHandle uintptr

// cimFsWriter represents a writer to a single CimFS filesystem instance. On disk, the image is
// composed of a filesystem file and several object ID and region files.
type cimFsWriter struct {
	// name of this cim. Usually a <name>.cim file will be created to represent this cim.
	name string
	// handle is the CIMFS_IMAGE_HANDLE that must be passed when calling CIMFS APIs.
	handle fsHandle
	// name of the active file i.e the file to which we are currently writing.
	activeName string
	// stream to currently active file.
	activeStream streamHandle
	// amount of bytes that can be written to the activeStream.
	activeLeft int64
}

// creates a new cim image. The handle returned in the `cim.handle` variable can then
// be used to do operations on this cim.
func create(imagePath string, oldFSName string, newFSName string) (_ *cimFsWriter, err error) {
	var oldNameBytes *uint16
	fsName := oldFSName
	if oldFSName != "" {
		oldNameBytes, err = windows.UTF16PtrFromString(oldFSName)
		if err != nil {
			return nil, err
		}
	}
	var newNameBytes *uint16
	if newFSName != "" {
		fsName = newFSName
		newNameBytes, err = windows.UTF16PtrFromString(newFSName)
		if err != nil {
			return nil, err
		}
	}
	var handle fsHandle
	if err := cimCreateImage(imagePath, oldNameBytes, newNameBytes, &handle); err != nil {
		return nil, err
	}
	return &cimFsWriter{handle: handle, name: filepath.Join(imagePath, fsName)}, nil
}

// creates alternate stream of given size at the given path
// relative to the cim path. This will replace the current active
// stream. Always, finish writing current active stream and then
// create an alternate stream.
func (c *cimFsWriter) createAlternateStream(path string, size uint64) (err error) {
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
func (c *cimFsWriter) closeStream() error {
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
func (c *cimFsWriter) addFile(path string, info *winio.FileBasicInfo, fileSize int64, securityDescriptor []byte, extendedAttributes []byte, reparseData []byte) error {
	err := c.closeStream()
	if err != nil {
		return err
	}
	fileMetadata := &cimFsFileMetadata{
		Attributes:     info.FileAttributes,
		FileSize:       fileSize,
		CreationTime:   info.CreationTime,
		LastWriteTime:  info.LastWriteTime,
		ChangeTime:     info.ChangeTime,
		LastAccessTime: info.LastAccessTime,
	}
	if len(securityDescriptor) == 0 {
		// Passing an empty security descriptor creates a CIM in a weird state.
		// Pass the NULL DACL.
		securityDescriptor = nullSd
	}
	fileMetadata.SecurityDescriptorBuffer = unsafe.Pointer(&securityDescriptor[0])
	fileMetadata.SecurityDescriptorSize = uint32(len(securityDescriptor))
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
	//TODO(ambarve): Docker adds sparse file tag to files which have some data in it. However,
	// this is not allowed in cimfs. Figure out how this can be fixed.
	// if info.FileAttributes&(FILE_ATTRIBUTE_DIRECTORY|FILE_ATTRIBUTE_SPARSE_FILE) == 0 {
	if info.FileAttributes&(windows.FILE_ATTRIBUTE_DIRECTORY) == 0 {
		c.activeLeft = fileSize
	}
	return nil
}

// This is a helper function which reads the file on host at path `hostPath` and adds it inside
// the cim at path `pathInCim`. If a file already exists inside cim at path `pathInCim` it will be
// overwritten.
func (c *cimFsWriter) addFileFromPath(pathInCim, hostPath string, securityDescriptor []byte, extendedAttributes []byte, reparseData []byte) error {
	f, err := os.Open(hostPath)
	if err != nil {
		return fmt.Errorf("failure when opening file at %s: %s", hostPath, err)
	}
	defer f.Close()

	basicInfo, err := winio.GetFileBasicInfo(f)
	if err != nil {
		return fmt.Errorf("failure when getting basic info for file %s: %s", hostPath, err)
	}

	replaceData, err := ioutil.ReadFile(hostPath)
	if err != nil {
		return fmt.Errorf("failed to read replacement file at %s : %s", hostPath, err)
	}

	if err := c.addFile(pathInCim, basicInfo, int64(len(replaceData)), securityDescriptor, extendedAttributes, reparseData); err != nil {
		return err
	}

	if _, err := c.Write(replaceData); err != nil {
		return fmt.Errorf("failed to write contents of file %s in cim: %s", pathInCim, err)
	}
	return nil
}

// write writes bytes to the active stream.
func (c *cimFsWriter) Write(p []byte) (int, error) {
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
func (c *cimFsWriter) addLink(oldPath string, newPath string) error {
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
func (c *cimFsWriter) unlink(path string) error {
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

func (c *cimFsWriter) commit() error {
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
func (c *cimFsWriter) close() error {
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
