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
	"github.com/Microsoft/hcsshim/internal/oc"
	"github.com/Microsoft/hcsshim/internal/wclayer"
	"github.com/Microsoft/hcsshim/osversion"
	"go.opencensus.io/trace"
)

// CimLayerWriter is an implementation of the wclayer.LayerWriter interface.
type CimLayerWriter struct {
	ctx context.Context
	s   *trace.Span
	// path to the layer (i.e layer's directory) as provided by the caller.
	// Even if a layer is stored as a cim in the cim directory, some files associated
	// with a layer are still stored in this path.
	path string
	// parent layer paths
	parentLayerPaths []string
	// Handle to the layer cim
	cimLayer *cim
	// pointer to the currently active hive file. When writing a normal file this will be nil
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

func (cw *CimLayerWriter) closeActiveHive() (err error) {
	if cw.activeHive != nil {
		err = cw.activeHive.Close()
		cw.activeHive = nil
	}
	return
}

// Add adds a file to the layer with given metadata.
func (cw *CimLayerWriter) Add(name string, fileInfo winio.FileBasicInfo, fileSize int64, securityDescriptor []byte, extendedAttributes []byte, reparseData []byte) error {
	if err := cw.closeActiveHive(); err != nil {
		return err
	}

	// if it is a delta hive file write it to the layer directory. Hives are handled differently.
	if isDeltaHive(name) {
		activeHive, err := os.Create(filepath.Join(cw.path, filepath.Base(name)))
		if err != nil {
			return fmt.Errorf("error creating delta hive %s: %s", name, err)
		}
		cw.activeHive = activeHive
		return nil
	} else {
		// for some reason containerd sends addFile requests with REPARSE_POINT attribute set but
		// empty reparse data. If that is the case reset REPARSE_POINT bit.
		if (fileInfo.FileAttributes&FILE_ATTRIBUTE_REPARSE_POINT) > 0 && len(reparseData) == 0 {
			fileInfo.FileAttributes &^= uint32(FILE_ATTRIBUTE_REPARSE_POINT)
		}
		return cw.cimLayer.addFile(toNtPath(name), fileInfo, fileSize, securityDescriptor, extendedAttributes, reparseData)
	}
}

// AddLink adds a hard link to the layer. The target must already have been added.
func (cw *CimLayerWriter) AddLink(name string, target string) error {
	name = toNtPath(name)
	target = toNtPath(target)
	if err := cw.closeActiveHive(); err != nil {
		return err
	}
	if isDeltaHive(name) {
		return fmt.Errorf("invalid link from %s to %s", name, target)
	}
	return cw.cimLayer.addLink(target, name)
}

// AddAlternateStream creates another alternate stream at the given
// path. Any writes made after this call will go to that stream.
func (cw *CimLayerWriter) AddAlternateStream(name string, size uint64) error {
	name = toNtPath(name)
	if err := cw.closeActiveHive(); err != nil {
		return err
	}
	return cw.cimLayer.createAlternateStream(name, size)
}

// Remove removes a file that was present in a parent layer from the layer.
func (cw *CimLayerWriter) Remove(name string) error {
	if err := cw.closeActiveHive(); err != nil {
		return err
	}
	return cw.cimLayer.unlink(toNtPath(name))
}

// Write writes data to the current file. The data must be in the format of a Win32
// backup stream.
func (cw *CimLayerWriter) Write(b []byte) (int, error) {
	if cw.activeHive != nil {
		return cw.activeHive.Write(b)
	} else {
		return cw.cimLayer.write(b)
	}
}

// ProcessImageEx function internally calls some functions which expect the base hives
// to be present at the given location in the layer path. These files will not actually be used
// but we need to fake them so that ProcessImageEx doesn't throw an error.
// The real registry files in the layer are created by `createHivesForBaseLayer` inside the cim.
func (cw *CimLayerWriter) createPlaceholderHivesForBaseLayer(layerPath string) error {
	regDir := filepath.Join(layerPath, regFilesPath)
	if err := os.MkdirAll(regDir, 0777); err != nil {
		return fmt.Errorf("error while creating placeholder registry hives directory: %s", err)
	}
	for _, hv := range hives {
		if _, err := os.Create(filepath.Join(regDir, hv.name)); err != nil {
			return fmt.Errorf("error while creating registry value at: %s, %s", filepath.Join(regDir, hv.name), err)
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
	err := cw.cimLayer.addFile(toNtPath(hivesPath), hivesDirInfo, 0, []byte{}, []byte{}, []byte{})
	if err != nil {
		return fmt.Errorf("failed while creating hives directory in the cim")
	}
	for _, hv := range hives {
		err := cw.cimLayer.addLink(toNtPath(filepath.Join(regFilesPath, hv.name)),
			toNtPath(filepath.Join(hivesPath, hv.base)))
		if err != nil {
			return fmt.Errorf("failed while creating base registry hives in the cim: %s", err)
		}
	}
	return nil
}

// reads the file at path `filePath` inside the cim `cimPath` and copies it at
// `destinationPath`.
func fetchFileFromCim(cimPath, filePath, destinationPath string) (err error) {
	// open the cim file and read it.
	cimReader, err := Open(cimPath)
	if err != nil {
		return fmt.Errorf("failed to open the cim %s: %s", cimPath, err)
	}
	defer func() {
		if err2 := cimReader.Close(); err == nil {
			err = err2
		}
	}()
	cimFile, err := cimReader.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed while opening file %s inside the cim %s: %s", filePath, cimPath, err)
	}
	fileData := make([]byte, cimFile.Size())
	rc, err := cimFile.Read(fileData)
	if err != nil && err != io.EOF {
		return fmt.Errorf("failed while reading %s: %s", cimFile.Name(), err)
	} else if uint64(rc) != cimFile.Size() {
		return fmt.Errorf("read truncated for file %s", cimFile.Name())
	}

	// create the destination file and write to it.
	destinationFile, err := os.Create(destinationPath)
	if err != nil {
		return fmt.Errorf("failed to created file %s: %s", destinationPath, err)
	}
	defer func() {
		if err2 := destinationFile.Close(); err == nil {
			err = err2
		}
	}()
	wc, err := destinationFile.Write(fileData)
	if err != nil {
		return fmt.Errorf("failed while writing to file %s: %s", destinationPath, err)
	} else if wc != rc {
		return fmt.Errorf("write truncated for file %s", destinationPath)
	}
	return
}

// merges the hive located at parentHivePath with the hive located at deltaHivePath and stores
// the result into the file at mergedHivePath
func mergeHive(parentHivePath, deltaHivePath, mergedHivePath string) (err error) {
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
	if err := orSaveHive(mergedHive, mergedHivePath, uint32(osversion.Get().MajorVersion), uint32(osversion.Get().MinorVersion)); err != nil {
		return fmt.Errorf("failed to save hive: %s", err)
	}
	return
}

// merges the delta hives of current layer with the registry hives of its parent layer.
func (cw *CimLayerWriter) mergeWithParentLayerHives(parentCimPath string) error {
	// create a temp directory to store parent layer hive files
	tmpParentLayer, err := ioutil.TempDir("", "")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %s", tmpParentLayer)
	}
	defer os.RemoveAll(tmpParentLayer)

	// create a temp directory to create merged hive files of the current layer
	tmpCurrentLayer, err := ioutil.TempDir("", "")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %s", tmpCurrentLayer)
	}
	defer os.RemoveAll(tmpCurrentLayer)

	// create parent layer hive files
	for _, hv := range hives {
		err := fetchFileFromCim(parentCimPath, filepath.Join(hivesPath, hv.base), filepath.Join(tmpParentLayer, hv.base))
		if err != nil {
			return err
		}
	}

	// merge hives
	for _, hv := range hives {
		err := mergeHive(filepath.Join(tmpParentLayer, hv.base), filepath.Join(cw.path, hv.delta), filepath.Join(tmpCurrentLayer, hv.base))
		if err != nil {
			return err
		}
	}

	// add merged hives into the cim layer
	mergedHives, err := ioutil.ReadDir(tmpCurrentLayer)
	if err != nil {
		return fmt.Errorf("failed to enumerate hive files: %s", err)
	}
	for _, hv := range mergedHives {
		cimHivePath := filepath.Join(hivesPath, hv.Name())
		err := cw.cimLayer.addFile(toNtPath(cimHivePath), winio.FileBasicInfo{}, hv.Size(), []byte{}, []byte{}, []byte{})
		if err != nil {
			return err
		}
		data, err := ioutil.ReadFile(filepath.Join(tmpCurrentLayer, hv.Name()))
		if err != nil {
			return fmt.Errorf("failed to read hive file %s: %s", filepath.Join(tmpCurrentLayer, hv.Name()), err)
		}
		if _, err := cw.cimLayer.write(data); err != nil {
			return fmt.Errorf("failed to write to hive: %s", err)
		}
		if err := cw.cimLayer.closeStream(); err != nil {
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
	err := cw.cimLayer.addFile(toNtPath(layoutFilesPath), layoutFileInfo, 15, []byte{}, []byte{}, []byte{})
	if err != nil {
		return fmt.Errorf("failed while creating layout file: %s", err)
	}
	cw.cimLayer.write([]byte(layoutData))
	if err != nil {
		return fmt.Errorf("failed while writing to layout file: %s", err)
	}
	return nil
}

// Close finishes the layer writing process and releases any resources.
func (cw *CimLayerWriter) Close(ctx context.Context) error {
	if err := cw.closeActiveHive(); err != nil {
		return err
	}
	// if this is a base layer then setup the hives folder as well
	if len(cw.parentLayerPaths) == 0 {
		if err := cw.createHivesForBaseLayer(); err != nil {
			return err
		}
		if err := cw.createLayoutFile(); err != nil {
			return err
		}
		// ProcessImageEx creates the scratch vhd for the base layer. It expects the
		// hive files in the layer path but in case of the cim the hives are stored
		// inside the cim. So we create empty placeholder hives inside the layer directory
		// before calling ProcessImageEx. 20 GB is the hard coded size of the base vhd file.
		if err := cw.createPlaceholderHivesForBaseLayer(cw.path); err != nil {
			return err
		}
		if err := wclayer.ProcessImageEx(ctx, cw.path, wclayer.ImageTypeBase, 20, wclayer.ProcessImage_NoOptions, cw.path); err != nil {
			return err
		}
	} else {
		if err := cw.mergeWithParentLayerHives(GetCimPathFromLayer(cw.parentLayerPaths[0])); err != nil {
			return err
		}

	}
	return cw.cimLayer.close()
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
	cimDirPath := GetCimDirFromLayer(path)
	if _, err = os.Stat(cimDirPath); os.IsNotExist(err) {
		// create cim directory
		if err = os.Mkdir(cimDirPath, 0755); err != nil {
			return nil, fmt.Errorf("failed while creating cim layers directory: %s", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("unable to access cim layers directory: %s", err)

	}

	if len(parentLayerPaths) > 0 {
		parentCim = GetCimNameFromLayer(parentLayerPaths[0])
	}

	cim, err := create(GetCimDirFromLayer(path), parentCim, GetCimNameFromLayer(path))
	if err != nil {
		return nil, fmt.Errorf("error in creating a new cim: %s", err)
	}
	return &CimLayerWriter{
		ctx:              ctx,
		s:                span,
		path:             path,
		parentLayerPaths: parentLayerPaths,
		cimLayer:         cim,
	}, nil
}
