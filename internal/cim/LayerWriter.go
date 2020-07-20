package cim

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Microsoft/go-winio"
	"github.com/Microsoft/hcsshim/internal/mylogger"
	"github.com/Microsoft/hcsshim/internal/oc"
	"github.com/Microsoft/hcsshim/internal/wclayer"
	"go.opencensus.io/trace"
)

// CimLayerWriter is just a wrapper around cim writer
type CimLayerWriter struct {
	ctx context.Context
	s   *trace.Span
	// directory path in which layer files other than cim file of the layer are stored.
	path string
	// parent layer paths
	parentLayerPaths []string
	// Handle to the layer cim
	layer *cim
	// poniter to the currently active hive file. When writing a normal file this will be nil
	activeHive *os.File
}

const (
	regFilesPath = "Files\\Windows\\System32\\config"
	hivesPath    = "Hives"
)

type hive struct {
	name  string
	base  string
	delta string
}

var (
	hives = []hive{
		{"SYSTEM", "SYSTEM_BASE", "SYSTEM_DELTA"},
		{"SOFTWARE", "SOFTWARE_BASE", "SOFTWARE_DELTA"},
		{"SAM", "SAM_BASE", "SAM_DELTA"},
		{"SECURITY", "SECURITY_BASE", "SECURITY_DELTA"},
		{"DEFAULT", "DEFAULTUSER_BASE", "DEFAULTUSER_DELTA"},
	}
)

func isDeltaHive(path string) bool {
	for _, hv := range hives {
		if strings.EqualFold(filepath.Base(path), hv.delta) {
			return true
		}
	}
	return false
}

// Add adds a file to the layer with given metadata.
func (cw *CimLayerWriter) Add(name string, fileInfo winio.FileBasicInfo, fileSize int64, securityDescriptor []byte, extendedAttributes []byte, reparseData []byte) error {
	mylogger.LogFmt("cim Add, name: %s, size: %d, fileinfo: %v\n", name, fileSize, fileInfo)

	if cw.activeHive != nil {
		cw.activeHive.Close()
		cw.activeHive = nil
	}

	// if it is a delta hive file write it to the layer directory. Hives are handled differently.
	if isDeltaHive(name) {
		var err error
		mylogger.LogFmt("creating the delta hive at: %s\n", filepath.Join(cw.path, filepath.Base(name)))
		cw.activeHive, err = os.Create(filepath.Join(cw.path, filepath.Base(name)))
		if err != nil {
			return fmt.Errorf("error creating delta hive %s: %s", name, err)
		}
		return nil
	} else {
		if (fileInfo.FileAttributes&FILE_ATTRIBUTE_REPARSE_POINT) > 0 && len(reparseData) == 0 {
			fileInfo.FileAttributes &^= uint32(FILE_ATTRIBUTE_REPARSE_POINT)
			mylogger.LogFmt("Had reparse point but empty reparse buffer. New attributes: %d\n", fileInfo.FileAttributes)
		}
		return cw.layer.addFile(toNtPath(name), fileInfo, fileSize, securityDescriptor, extendedAttributes, reparseData)
	}
}

// AddLink adds a hard link to the layer. The target must already have been added.
func (cw *CimLayerWriter) AddLink(name string, target string) error {
	name = toNtPath(name)
	target = toNtPath(target)
	if isDeltaHive(name) {
		return fmt.Errorf("invalid link from %s to %s", name, target)
	}
	mylogger.LogFmt("cim AddLink, name: %s, target: %s\n", name, target)
	return cw.layer.addLink(name, target)
}

// AddAlternateStream creates another alternate stream at the given
// path. Any writes made after this call will go to that stream.
func (cw *CimLayerWriter) AddAlternateStream(name string, size uint64) error {
	name = toNtPath(name)
	mylogger.LogFmt("cim AddAlternateStream, name: %s, size: %d\n", name, size)
	return cw.layer.createAlternateStream(name, size)
}

// Remove removes a file that was present in a parent layer from the layer.
func (cw *CimLayerWriter) Remove(name string) error {
	// if name == "Files\\Windows\\System32\\LogFiles\\WMI\\RtBackup\\etwrteventlog-security.etl" {
	// 	mylogger.LogFmt("Forcefully closing cim\n")
	// 	cw.Close()
	// 	return fmt.Errorf("Forcefully closing cim")
	// }
	mylogger.LogFmt("cim Remove, name: %s\n", name)
	return cw.layer.unlink(toNtPath(name))
}

// Write writes data to the current file. The data must be in the format of a Win32
// backup stream.
func (cw *CimLayerWriter) Write(b []byte) (int, error) {
	mylogger.LogFmt("cim write, size: %d\n", len(b))
	if cw.activeHive != nil {
		return cw.activeHive.Write(b)
	} else {
		return cw.layer.write(b)
	}
}

// ProcessImageEx function internally calls some functions which expect the base hives
// to be present at the given location in the layer path. These files will not actually be used
// but we need to fake them so that ProcessImageEx doesn't throw an error.
// The real registry files in the layer are created by `createHivesForBaseLayer` inside the cim.
func (cw *CimLayerWriter) createPlaceholderHivesForBaseLayer(layerPath string) error {
	regDir := filepath.Join(layerPath, regFilesPath)
	if err := os.MkdirAll(regDir, 0777); err != nil {
		return fmt.Errorf("error while creating fake registry hives: %s", err)
	}
	for _, hv := range hives {
		if _, err := os.Create(filepath.Join(regDir, hv.name)); err != nil {
			return fmt.Errorf("error while creating regsitry value at: %s, %s", filepath.Join(regDir, hv.name), err)
		}
	}
	return nil
}

// creates the base registry hives inside the cim.
func (cw *CimLayerWriter) createHivesForBaseLayer() error {
	// make hives directory
	hivesDirInfo := winio.FileBasicInfo{
		CreationTime:   syscall.NsecToFiletime(time.Now().UnixNano()),
		LastAccessTime: syscall.NsecToFiletime(time.Now().UnixNano()),
		LastWriteTime:  syscall.NsecToFiletime(time.Now().UnixNano()),
		ChangeTime:     syscall.NsecToFiletime(time.Now().UnixNano()),
		FileAttributes: 16,
	}
	err := cw.layer.addFile(toNtPath(hivesPath), hivesDirInfo, 0, []byte{}, []byte{}, []byte{})
	if err != nil {
		return fmt.Errorf("failed while creating hives directory\n")
	}
	mylogger.LogFmt("create registry hives.....\n")
	for _, hv := range hives {
		// TODO: order of arguments is reversed in this call to addLink
		// figure out what is wrong here
		err := cw.layer.addLink(toNtPath(filepath.Join(hivesPath, hv.base)),
			toNtPath(filepath.Join(regFilesPath, hv.name)))
		if err != nil {
			return fmt.Errorf("failed while creating base registry hives: %s", err)
		}
	}
	return nil
}

// merges the delta hives of current layer with the registry hives of its parent layer.
func (cw *CimLayerWriter) mergeWithParentLayerHives(parentCimPath string) error {
	// create a temp directory to create parent layer hive files
	tmpParentLayer, err := ioutil.TempDir("", "")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %s", tmpParentLayer)
	}
	// defer os.RemoveAll(tmpParentLayer)
	// create a temp directory to create merged hive files of the current layer
	tmpCurrentLayer, err := ioutil.TempDir("", "")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %s", tmpCurrentLayer)
	}
	// defer os.RemoveAll(tmpCurrentLayer)
	mylogger.LogFmt("merging layer: %s hives with parent layer: %s, parent tmp: %s, current tmp: %s", cw.path, parentCimPath, tmpParentLayer, tmpCurrentLayer)

	// open the cim file for reading
	cimReader, err := Open(parentCimPath)
	if err != nil {
		return fmt.Errorf("failed to open the cim file: %s", err)
	}
	// open the hives dir of the cim
	hivesDir, err := cimReader.Open(hivesPath)
	if err != nil {
		return fmt.Errorf("error opening hives directory of the cim: %s", err)
	}

	// create parent layer hive files
	for _, hv := range hives {
		cimHive, err := hivesDir.OpenAt(hv.base)
		fileData := make([]byte, cimHive.Size())
		rc, err := cimHive.Read(fileData)
		if err != nil && err != io.EOF {
			fmt.Errorf("failed with %s while reading %s", err, cimHive.name)
		} else if uint64(rc) != cimHive.Size() {
			fmt.Errorf("couldn't read complete file contents for file %s", cimHive.name)
		}
		// write hive to a temp file
		tmpHive, err := os.Create(filepath.Join(tmpParentLayer, hv.base))
		if err != nil {
			fmt.Errorf("failed to created temporary hive file %s: %s", filepath.Join(tmpParentLayer, hv.base), err)
		}
		wc, err := tmpHive.Write(fileData)
		if err != nil {
			fmt.Errorf("failed while writing to temporary hive file %s: %s", tmpHive.Name(), err)
		} else if wc != rc {
			fmt.Errorf("couldn't write complete file contents for file %s", tmpHive.Name())
		}
		if err := tmpHive.Close(); err != nil {
			fmt.Errorf("failed to close hive file: %s", filepath.Join(tmpParentLayer, hv.base))
		}
	}

	// merge hives
	for _, hv := range hives {
		parentHivePath := filepath.Join(tmpParentLayer, hv.base)
		mergedHivePath := filepath.Join(tmpCurrentLayer, hv.base)
		deltaHivePath := filepath.Join(cw.path, hv.delta)
		var baseHive, deltaHive, mergedHive orHKey
		if err := orOpenHive(parentHivePath, &baseHive); err != nil {
			return fmt.Errorf("failed to open base hive %s: %s", parentHivePath, err)
		}
		defer func() {
			err2 := orCloseHive(baseHive)
			if err == nil {
				err = fmt.Errorf("failed to close base hive: %s", err2)
			}
		}()
		if err := orOpenHive(deltaHivePath, &deltaHive); err != nil {
			return fmt.Errorf("failed to open delta hive %s: %s", deltaHivePath, err)
		}
		defer func() {
			err2 := orCloseHive(deltaHive)
			if err == nil {
				err = fmt.Errorf("failed to close delta hive: %s", err2)
			}
		}()
		if err := orMergeHives([]orHKey{baseHive, deltaHive}, &mergedHive); err != nil {
			return fmt.Errorf("failed to merge hives: %s", err)
		}
		defer func() {
			err2 := orCloseHive(mergedHive)
			if err == nil {
				err = fmt.Errorf("failed to close merged hive: %s", err2)
			}
		}()
		// TODO(ambarve): Add proper constants for 6,1 (version values)
		if err := orSaveHive(mergedHive, mergedHivePath, 6, 1); err != nil {
			return fmt.Errorf("failed to save hive: %s", err)
		}
	}

	// now add these hives into the cim layer
	mergedHives, err := ioutil.ReadDir(tmpCurrentLayer)
	if err != nil {
		return fmt.Errorf("failed to enumerate hive files: %s", err)
	}
	for _, hv := range mergedHives {
		cimHivePath := filepath.Join(hivesPath, hv.Name())
		cw.layer.addFile(toNtPath(cimHivePath), winio.FileBasicInfo{}, hv.Size(), []byte{}, []byte{}, []byte{})
		data, err := ioutil.ReadFile(filepath.Join(tmpCurrentLayer, hv.Name()))
		if err != nil {
			return fmt.Errorf("failed to read hive file %s: %s", filepath.Join(tmpCurrentLayer, hv.Name()), err)
		}
		if _, err := cw.layer.write(data); err != nil {
			return fmt.Errorf("failed to write to hive: %s", err)
		}
		if err := cw.layer.closeStream(); err != nil {
			return fmt.Errorf("failed to close stream: %s", err)
		}
	}
	return nil
}

func (cw *CimLayerWriter) createLayoutFile() error {
	layoutFilesPath := "layout"
	layoutData := "vhd-with-hives\n"
	layoutFileInfo := winio.FileBasicInfo{
		CreationTime:   syscall.NsecToFiletime(time.Now().UnixNano()),
		LastAccessTime: syscall.NsecToFiletime(time.Now().UnixNano()),
		LastWriteTime:  syscall.NsecToFiletime(time.Now().UnixNano()),
		ChangeTime:     syscall.NsecToFiletime(time.Now().UnixNano()),
		FileAttributes: 0,
	}
	err := cw.layer.addFile(toNtPath(layoutFilesPath), layoutFileInfo, 15, []byte{}, []byte{}, []byte{})
	if err != nil {
		return fmt.Errorf("failed while creating layout file: %s", err)
	}
	cw.layer.write([]byte(layoutData))
	if err != nil {
		return fmt.Errorf("failed while writing to layout file: %s", err)
	}
	return nil
}

// Close finishes the layer writing process and releases any resources.
func (cw *CimLayerWriter) Close(ctx context.Context) error {
	mylogger.LogFmt("cim Close\n")
	if cw.activeHive != nil {
		cw.activeHive.Close()
		cw.activeHive = nil
	}
	// if this is a base layer then setup the hives folder as well
	if len(cw.parentLayerPaths) == 0 {
		if err := cw.createHivesForBaseLayer(); err != nil {
			return err
		}
		if err := cw.createLayoutFile(); err != nil {
			return err
		}
		// TODO(ambarve); for the path just pass in the path of the layer
		// directory (ProcessImageEx will create some files in that directory but
		// we can safely ignore them). 20 is the hard coded size of the base vhdx.
		// create the fake base hives before calling ProcessImageEx
		if err := cw.createPlaceholderHivesForBaseLayer(cw.path); err != nil {
			return err
		}
		if err := wclayer.ProcessImageEx(ctx, cw.path, wclayer.ImageTypeBase, 20, wclayer.ProcessImage_NoOptions, cw.path); err != nil {
			return err
		}
	} else {
		if err := cw.mergeWithParentLayerHives(GetCimPath(cw.parentLayerPaths[0])); err != nil {
			return err
		}

	}
	return cw.layer.close()
}

func NewCimLayerWriter(ctx context.Context, path string, parentLayerPaths []string) (_ *CimLayerWriter, err error) {
	ctx, span := trace.StartSpan(ctx, "hcsshim::NewCimLayerWriter")
	defer func() {
		if err != nil {
			oc.SetSpanStatus(span, err)
			span.End()
		}
	}()
	span.AddAttributes(
		trace.StringAttribute("path", path),
		trace.StringAttribute("parentLayerPaths", strings.Join(parentLayerPaths, ", ")))
	parentCim := ""
	cimDirPath := GetCimDir(path)
	if _, err = os.Stat(cimDirPath); os.IsNotExist(err) {
		// create cim directory
		// TODO(ambarve): use correct permissions here
		if err = os.Mkdir(cimDirPath, 0755); err != nil {
			return nil, fmt.Errorf("failed while creating cim layers directory: %s", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("unable to access cim layers directory: %s", err)
	}
	if len(parentLayerPaths) > 0 {
		mylogger.LogFmt("NewCimLayerWriter, path: %s, parentPath %s, ", path, parentLayerPaths[0])
		parentCim = GetCimName(parentLayerPaths[0])
	}
	mylogger.LogFmt("cimname: %s, cimdir: %s\n", GetCimDir(path), GetCimName(path))
	cim, err := create(GetCimDir(path), parentCim, GetCimName(path))
	if err != nil {
		return nil, fmt.Errorf("error in creating a new cim: %s", err)
	}
	return &CimLayerWriter{
		ctx:              ctx,
		s:                span,
		path:             path,
		parentLayerPaths: parentLayerPaths,
		layer:            cim,
	}, nil
}
