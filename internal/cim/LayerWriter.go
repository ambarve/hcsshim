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
	"github.com/Microsoft/go-winio/pkg/guid"
	"github.com/Microsoft/hcsshim/internal/mylogger"
	"github.com/Microsoft/hcsshim/internal/oc"
	"github.com/Microsoft/hcsshim/internal/wclayer"
	"github.com/Microsoft/hcsshim/osversion"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
)

// A CimLayer consist of cim files (which are usually stored in the `cim-layers` directory and
// some other files which are stored in the directory of that layer (i.e the `path` directory).
// CimLayerWriter is an implementation of the wclayer.LayerWriter interface.  When
// importing an image layer into a cim layer format this CimLayerWriter will write most of the
// files into the cim through the `cimLayer` instance and will write some of the files
// into the local filesystem (inside the `path` directory) through the `stdFileWriter` instance.
type CimLayerWriter struct {
	ctx context.Context
	s   *trace.Span
	// path to the layer (i.e layer's directory) as provided by the caller.
	// Even if a layer is stored as a cim in the cim directory, some files associated
	// with a layer are still stored in this path.
	path string
	// parent layer paths
	parentLayerPaths []string
	// Handle to the layer cim - writes to the cim file
	cimLayer *cim
	// Handle to the writer for writing files in the local filesystem
	stdFileWriter *StdFileWriter
	// reference to currently active writer
	activeWriter io.Writer
}

const (
	regFilesPath        = "Files\\Windows\\System32\\config"
	hivesPath           = "Hives"
	utilityVMPath       = "UtilityVM"
	utilityVMFilesPath  = "UtilityVM\\Files"
	bcdFilePath         = "UtilityVM\\Files\\EFI\\Microsoft\\Boot\\BCD"
	containerBaseVhd    = "blank-base.vhdx"
	containerScratchVhd = "blank.vhdx"
	utilityVMBaseVhd    = "SystemTemplateBase.vhdx"
	utilityVMScratchVhd = "SystemTemplate.vhdx"
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

// checks if this particular file should be written with a stdFileWriter instead of
// using the cimWriter.
func isStdFile(path string) bool {
	return (isDeltaHive(path) || path == bcdFilePath)
}

// Add adds a file to the layer with given metadata.
func (cw *CimLayerWriter) Add(name string, fileInfo *winio.FileBasicInfo, fileSize int64, securityDescriptor []byte, extendedAttributes []byte, reparseData []byte) error {
	mylogger.LogFmt("LayerWriter Add: %s, fileInfo: %+v, fileSize: %d\n", name, fileInfo, fileSize)
	if isStdFile(name) {
		if err := cw.stdFileWriter.Add(name, fileInfo); err != nil {
			return err
		}
		cw.activeWriter = cw.stdFileWriter
	} else {
		// for some reason containerd sends addFile requests with REPARSE_POINT attribute set but
		// empty reparse data. If that is the case reset REPARSE_POINT bit.
		if (fileInfo.FileAttributes&FILE_ATTRIBUTE_REPARSE_POINT) > 0 && len(reparseData) == 0 {
			fileInfo.FileAttributes &^= uint32(FILE_ATTRIBUTE_REPARSE_POINT)
		}
		if err := cw.cimLayer.addFile(toNtPath(name), fileInfo, fileSize, securityDescriptor, extendedAttributes, reparseData); err != nil {
			return err
		}
		cw.activeWriter = cw.cimLayer
	}
	return nil
}

// AddLink adds a hard link to the layer. The target must already have been added.
func (cw *CimLayerWriter) AddLink(name string, target string) error {
	name = toNtPath(name)
	target = toNtPath(target)
	mylogger.LogFmt("LayerWriter AddLink: name: %s, target: %s\n", name, target)
	if isStdFile(name) {
		return cw.stdFileWriter.AddLink(name, target)
	} else {
		return cw.cimLayer.addLink(target, name)
	}
}

// AddAlternateStream creates another alternate stream at the given
// path. Any writes made after this call will go to that stream.
func (cw *CimLayerWriter) AddAlternateStream(name string, size uint64) error {
	name = toNtPath(name)
	cw.activeWriter = cw.cimLayer
	return cw.cimLayer.createAlternateStream(name, size)
}

// Remove removes a file that was present in a parent layer from the layer.
func (cw *CimLayerWriter) Remove(name string) error {
	mylogger.LogFmt("LayerWriter Remove: name: %s\n", name)
	if isStdFile(name) {
		return cw.stdFileWriter.Remove(name)
	} else {
		return cw.cimLayer.unlink(toNtPath(name))
	}
}

// Write writes data to the current file. The data must be in the format of a Win32
// backup stream.
func (cw *CimLayerWriter) Write(b []byte) (int, error) {
	mylogger.LogFmt("LayerWriter write %d bytes\n", len(b))
	return cw.activeWriter.Write(b)
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
	hivesDirInfo := &winio.FileBasicInfo{
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
			err = errors.Wrap(err2, "failed to close base hive")
		}
	}()
	if err := orOpenHive(deltaHivePath, &deltaHive); err != nil {
		return fmt.Errorf("failed to open delta hive %s: %s", deltaHivePath, err)
	}
	defer func() {
		err2 := orCloseHive(deltaHive)
		if err == nil {
			err = errors.Wrap(err2, "failed to close delta hive")
		}
	}()
	if err := orMergeHives([]orHKey{baseHive, deltaHive}, &mergedHive); err != nil {
		return fmt.Errorf("failed to merge hives: %s", err)
	}
	defer func() {
		err2 := orCloseHive(mergedHive)
		if err == nil {
			err = errors.Wrap(err2, "failed to close merged hive")
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
		err := mergeHive(filepath.Join(tmpParentLayer, hv.base), filepath.Join(cw.path, hivesPath, hv.delta), filepath.Join(tmpCurrentLayer, hv.base))
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
		err := cw.cimLayer.addFile(toNtPath(cimHivePath), &winio.FileBasicInfo{}, hv.Size(), []byte{}, []byte{}, []byte{})
		if err != nil {
			return err
		}
		data, err := ioutil.ReadFile(filepath.Join(tmpCurrentLayer, hv.Name()))
		if err != nil {
			return fmt.Errorf("failed to read hive file %s: %s", filepath.Join(tmpCurrentLayer, hv.Name()), err)
		}
		if _, err := cw.cimLayer.Write(data); err != nil {
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
	layoutFileInfo := &winio.FileBasicInfo{
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
	cw.cimLayer.Write([]byte(layoutData))
	if err != nil {
		return fmt.Errorf("failed while writing to layout file: %s", err)
	}
	return nil
}

// Close finishes the layer writing process and releases any resources.
func (cw *CimLayerWriter) Close(ctx context.Context) error {
	mylogger.LogFmt("closing layer %s, parent layers: %v\n", cw.path, cw.parentLayerPaths)
	if err := cw.stdFileWriter.Close(ctx); err != nil {
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
		// TODO(ambarve): We probably should reapply the timestamps for the hives directory.
		if err := cw.mergeWithParentLayerHives(GetCimPathFromLayer(cw.parentLayerPaths[0])); err != nil {
			return err
		}

	}

	// Cim write done. We still have to update the bcd with the diskID and partition ID of the
	// scratch vhd but that vhd is created by ProcessImageEx call below. so we must close the cim
	// now, finish the ProcessImageEx call and then reopen the cim to edit the bcd file.
	if err := cw.cimLayer.close(); err != nil {
		return err
	}

	// Mount the cim here and call process utility image
	layerGUID, err := guid.NewV4()
	if err != nil {
		return fmt.Errorf("error creating guid: %s", err)
	}
	if err := cimMountImage(GetCimDirFromLayer(cw.path), GetCimNameFromLayer(cw.path), 0, &layerGUID); err != nil {
		return fmt.Errorf("failed to mount cim at: %s:%s\n", GetCimDirFromLayer(cw.path), GetCimNameFromLayer(cw.path))
	}

	if err := wclayer.ProcessImageEx(ctx, filepath.Join(fmt.Sprintf("\\\\?\\Volume{%s}", layerGUID), utilityVMPath), wclayer.ImageTypeUtility, 20, wclayer.ProcessImage_SkipUpdateBcdForBoot|wclayer.ProcessImage_NoOptimizeCiCache, filepath.Join(cw.path, utilityVMPath)); err != nil {
		return err
	}
	if err := cimDismountImage(&layerGUID); err != nil {
		return fmt.Errorf("failed to dismount cim: %s", err)
	}

	diskID, partitionID, err := wclayer.GetScratchDriveDiskIDPartitionID(ctx, filepath.Join(cw.path, utilityVMPath, utilityVMBaseVhd))
	if err != nil {
		fmt.Errorf("failed to get base vhd layout info: %s", err)
	}

	diskIDGUID, err := guid.FromString(diskID)
	if err != nil {
		return fmt.Errorf("error parsing uuid(%s): %s", diskID, err)
	}
	partitionGUID, err := guid.FromString(partitionID)
	if err != nil {
		return fmt.Errorf("error parsing uuid(%s): %s", partitionID, err)
	}

	// Update the BCD for utility VM image and write it inside the cim
	if err := wclayer.UpdateBcdStoreForBoot(ctx, filepath.Join(cw.path, utilityVMFilesPath), diskIDGUID, partitionGUID); err != nil {
		return fmt.Errorf("failed to update BCD: %s", err)
	}

	// //TODO(ambarve): rmeove this. only adding for debugging
	// cmd := exec.Command("bcdedit.exe", "/store", filepath.Join(cw.path, utilityVMFilesPath, "EFI\\Microsoft\\Boot\\BCD"), "/set", "{default}", "debug", "on")
	// var out bytes.Buffer
	// cmd.Stdout = &out
	// err = cmd.Run()
	// if err != nil {
	// 	mylogger.LogFmt("failed to setup bcd: %s", err)
	// } else {
	// 	mylogger.LogFmt("output : %q\n", out.String())
	// }

	// open cim again
	reopenedCim, err := create(GetCimDirFromLayer(cw.path), GetCimNameFromLayer(cw.path), "")
	bcdData, err := ioutil.ReadFile(filepath.Join(cw.path, bcdFilePath))
	if err != nil {
		return fmt.Errorf("failed to read BCD file at %s : %s", filepath.Join(cw.path, bcdFilePath), err)
	}
	if err := reopenedCim.addFile(toNtPath(bcdFilePath), &winio.FileBasicInfo{}, int64(len(bcdData)), []byte{}, []byte{}, []byte{}); err != nil {
		return fmt.Errorf("failed to updated BCD file inside cim: %s", err)
	}
	if _, err := reopenedCim.Write(bcdData); err != nil {
		return fmt.Errorf("failed to write BCD contents in cim: %s", err)
	}
	if err := reopenedCim.close(); err != nil {
		return fmt.Errorf("failed to close stream: %s", err)
	}
	return nil
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

	sfw, err := newStdFileWriter(path, parentLayerPaths)
	if err != nil {
		return nil, fmt.Errorf("error in creating new standard file writer: %s", err)
	}
	return &CimLayerWriter{
		ctx:              ctx,
		s:                span,
		path:             path,
		parentLayerPaths: parentLayerPaths,
		cimLayer:         cim,
		stdFileWriter:    sfw,
	}, nil
}
