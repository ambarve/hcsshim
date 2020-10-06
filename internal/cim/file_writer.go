package cim

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/Microsoft/go-winio"
	"github.com/Microsoft/hcsshim/internal/mylogger"
	"github.com/Microsoft/hcsshim/internal/safefile"
	"github.com/Microsoft/hcsshim/internal/winapi"
	"github.com/pkg/errors"
)

type dirInfo struct {
	path     string
	fileInfo winio.FileBasicInfo
}

// StdFileWriter writes the files of a layer to the layer folder instead of writing them inside the cim.
// For some files (like the Hive files or UtilityVM files) it is necessary to write them as a normal file
// first, do some modifications on them (for example merging of hives or processing of UtilityVM files)
// and then write the modified versions into the cim. This writer is used for such files.
type StdFileWriter struct {
	activeFile *os.File
	// parent layer paths
	parentLayerPaths []string
	// path to the current layer
	path string
	// the open handle to the path directory
	root *os.File
	// // open handle to topmost parent
	// parentRoots  []*os.File
	// hasUtilityVM bool
	// // array of directories that are changed.
	// // TODO(ambarve): Since StdFileWriter is only used for UtilityVM files is it
	// // necessary to maintain and reapply these timestamps here?
	// changedDi []dirInfo
}

func newStdFileWriter(root string, parentRoots []string) (sfw *StdFileWriter, err error) {
	sfw = &StdFileWriter{
		path:             root,
		parentLayerPaths: parentRoots,
	}
	sfw.root, err = safefile.OpenRoot(root)
	if err != nil {
		return
	}
	// for _, r := range parentRoots {
	// 	f, err := safefile.OpenRoot(r)
	// 	if err != nil {
	// 		return sfw, err
	// 	}
	// 	sfw.parentRoots = append(sfw.parentRoots, f)
	// }
	return
}

// func (sfw *StdFileWriter) initUtilityVM() error {
// 	if !sfw.hasUtilityVM {
// 		mylogger.LogFmt("creating utilityvm directory at %s, in root %s\n", utilityVMPath, sfw.root.Name())
// 		err := safefile.MkdirRelative(utilityVMPath, sfw.root)
// 		if err != nil {
// 			return err
// 		}
// 		if len(sfw.parentLayerPaths) > 0 {
// 			// Server 2016 does not support multiple layers for the utility VM, so
// 			// clone the utility VM from the parent layer into this layer. Use hard
// 			// links to avoid unnecessary copying, since most of the files are
// 			// immutable.
// 			err = wclayer.CloneTree(sfw.parentRoots[0], sfw.root, utilityVMFilesPath, wclayer.MutatedUtilityVMFiles)
// 			if err != nil {
// 				return fmt.Errorf("cloning the parent utility VM image failed: %s", err)
// 			}
// 		}
// 		sfw.hasUtilityVM = true
// 	}
// 	return nil
// }

func (sfw *StdFileWriter) closeActiveFile() (err error) {
	if sfw.activeFile != nil {
		mylogger.LogFmt("closing currently active file\n")
		err = sfw.activeFile.Close()
		sfw.activeFile = nil
	}
	return
}

// func (sfw *StdFileWriter) addUtilityVmFile(name string, fileInfo *winio.FileBasicInfo) error {
// 	name = filepath.Clean(name)
// 	if !sfw.hasUtilityVM {
// 		return errors.New("missing UtilityVM directory")
// 	}
// 	if !strings.HasPrefix(name, utilityVMFilesPath) && name != utilityVMFilesPath {
// 		return errors.New("invalid UtilityVM layer")
// 	}
// 	createDisposition := uint32(winapi.FILE_OPEN)
// 	if (fileInfo.FileAttributes & syscall.FILE_ATTRIBUTE_DIRECTORY) != 0 {
// 		st, err := safefile.LstatRelative(name, sfw.root)
// 		if err != nil && !os.IsNotExist(err) {
// 			return err
// 		}
// 		if st != nil {
// 			// Delete the existing file/directory if it is not the same type as this directory.
// 			existingAttr := st.Sys().(*syscall.Win32FileAttributeData).FileAttributes
// 			if (uint32(fileInfo.FileAttributes)^existingAttr)&(syscall.FILE_ATTRIBUTE_DIRECTORY|syscall.FILE_ATTRIBUTE_REPARSE_POINT) != 0 {
// 				if err = safefile.RemoveAllRelative(name, sfw.root); err != nil {
// 					return err
// 				}
// 				st = nil
// 			}
// 		}
// 		if st == nil {
// 			if err = safefile.MkdirRelative(name, sfw.root); err != nil {
// 				return err
// 			}
// 		}
// 	} else {
// 		// Overwrite any existing hard link.
// 		err := safefile.RemoveRelative(name, sfw.root)
// 		if err != nil && !os.IsNotExist(err) {
// 			return err
// 		}
// 		createDisposition = winapi.FILE_CREATE
// 	}

// 	f, err := safefile.OpenRelative(
// 		name,
// 		sfw.root,
// 		// syscall.GENERIC_READ|syscall.GENERIC_WRITE|winio.WRITE_DAC|winio.WRITE_OWNER|winio.ACCESS_SYSTEM_SECURITY,
// 		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
// 		syscall.FILE_SHARE_READ,
// 		createDisposition,
// 		// winapi.FILE_OPEN_REPARSE_POINT,
// 		0,
// 	)
// 	if err != nil {
// 		return err
// 	}
// 	defer func() {
// 		if f != nil {
// 			mylogger.LogFmt("closing the f file\n")
// 			f.Close()
// 			safefile.RemoveRelative(name, sfw.root)
// 		}
// 	}()

// 	err = winio.SetFileBasicInfo(f, fileInfo)
// 	if err != nil {
// 		return err
// 	}

// 	sfw.activeFile = f
// 	f = nil
// 	return nil

// }

// Add adds a file to the layer with given metadata.
func (sfw *StdFileWriter) Add(name string, fileInfo *winio.FileBasicInfo) error {
	if err := sfw.closeActiveFile(); err != nil {
		return err
	}

	// The directory of this file might be created inside the cim.
	// make sure we have the same parent directory chain here
	if err := os.MkdirAll(filepath.Join(sfw.path, filepath.Dir(name)), 0755); err != nil {
		return fmt.Errorf("failed to create file %s: %s", name, err)
	}

	f, err := safefile.OpenRelative(
		name,
		sfw.root,
		syscall.GENERIC_READ|syscall.GENERIC_WRITE|winio.WRITE_DAC|winio.WRITE_OWNER|winio.ACCESS_SYSTEM_SECURITY,
		syscall.FILE_SHARE_READ,
		winapi.FILE_CREATE,
		winapi.FILE_OPEN_REPARSE_POINT,
	)
	if err != nil {
		return fmt.Errorf("error creating standard file %s: %s", name, err)
	}
	sfw.activeFile = f
	return nil
}

// AddLink adds a hard link to the layer. The target must already have been added.
func (sfw *StdFileWriter) AddLink(name string, target string) error {
	if err := sfw.closeActiveFile(); err != nil {
		return err
	}
	if strings.HasPrefix(name, hivesPath) {
		return errors.New("invalid hard link in layer")
	}
	return nil
}

// Remove removes a file that was present in a parent layer from the layer.
func (sfw *StdFileWriter) Remove(name string) error {
	if err := sfw.closeActiveFile(); err != nil {
		return err
	}
	return fmt.Errorf("invalid tombstone %s", name)
}

// Write writes data to the current file. The data must be in the format of a Win32
// backup stream.
func (sfw *StdFileWriter) Write(b []byte) (int, error) {
	return sfw.activeFile.Write(b)
}

// Close finishes the layer writing process and releases any resources.
func (sfw *StdFileWriter) Close(ctx context.Context) error {
	if err := sfw.closeActiveFile(); err != nil {
		return err
	}
	return nil
}
