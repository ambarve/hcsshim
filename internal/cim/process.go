package cim

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Microsoft/go-winio"
	"github.com/docker/docker/pkg/ioutils"
)

// createPlaceHolderHives Creates the empty place holder registry hives inside the layer
// directory pointed by `layerPath`.
// HCS APIs called by processBaseLayer expects the registry hive files in the layer
// directory at path `layerPath + regFilesPath` but in case of the cim the hives are
// stored inside the cim and the processBaseLayer call fails if it doesn't find those
// files so we create empty placeholder hives inside the layer directory
func createPlaceHolderHives(layerPath string) error {
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

// processBaseLayer takes care of the special handling (such as creating the VHDs,
// generating the reparse points, updating BCD store etc) that is required for the base
// layer of an image. This function takes care of that processing once all layer files are
// written to the cim. This function creates VHD files inside the directory pointed by
// `layerPath` and expects the the layer cim is present at the usual location retrieved by
// `GetCimPathFromLayer`.
func processBaseLayer(ctx context.Context, layerPath string) (err error) {
	// process container base layer
	if err = createPlaceHolderHives(layerPath); err != nil {
		return err
	}
	if err = setupContainerBaseLayer(ctx, layerPath); err != nil {
		return fmt.Errorf("failed to setup container base layer: %s", err)
	}

	// process utilityVM base layer
	// setupUtilityVMBaseLayer needs to access some of the layer files so we mount the cim
	// and pass the path of the mounted cim as layerpath to setupUtilityVMBaseLayer.
	mountpath, err := Mount(GetCimPathFromLayer(layerPath))
	if err != nil {
		return fmt.Errorf("failed to mount cim : %s", err)
	}
	defer func() {
		// Try to unmount irrespective of errors
		UnMount(GetCimPathFromLayer(layerPath))
	}()

	if err := setupUtilityVMBaseLayer(ctx, filepath.Join(mountpath, utilityVMPath), layerPath); err != nil {
		return fmt.Errorf("failed to setup utility vm base layer: %s", err)
	}
	if err = UnMount(GetCimPathFromLayer(layerPath)); err != nil {
		return fmt.Errorf("failed to dismount cim: %s", err)
	}
	return nil
}

// createBaseLayerHives creates the base registry hives inside the given cim.
func createBaseLayerHives(cimWriter *cimFsWriter) error {
	// make hives directory
	hivesDirInfo := &winio.FileBasicInfo{
		CreationTime:   syscall.NsecToFiletime(time.Now().UnixNano()),
		LastAccessTime: syscall.NsecToFiletime(time.Now().UnixNano()),
		LastWriteTime:  syscall.NsecToFiletime(time.Now().UnixNano()),
		ChangeTime:     syscall.NsecToFiletime(time.Now().UnixNano()),
		FileAttributes: 16,
	}
	err := cimWriter.addFile(hivesPath, hivesDirInfo, 0, []byte{}, []byte{}, []byte{})
	if err != nil {
		return fmt.Errorf("failed while creating hives directory in the cim")
	}
	// add hard links from base hive files.
	for _, hv := range hives {
		err := cimWriter.addLink(filepath.Join(regFilesPath, hv.name),
			filepath.Join(hivesPath, hv.base))
		if err != nil {
			return fmt.Errorf("failed while creating base registry hives in the cim: %s", err)
		}
	}
	return nil
}

// Some of the layer files that are generated during the processBaseLayer call must be added back
// inside the cim, some registry file links must be updated. This function takes care of all those
// steps.
// Note: This function opens the cim file for writing and updates it.
func postProcessBaseLayer(ctx context.Context, layerPath string) (err error) {
	// fetch some files from the cim before opening it for writing.
	tmpDir, err := ioutils.TempDir(layerPath, "")
	if err != nil {
		return fmt.Errorf("failed to create temporary directory at %s: %s", tmpDir, err)
	}
	defer os.RemoveAll(tmpDir)
	layerRelativeSystemHivePath := filepath.Join(utilityVMPath, regFilesPath, "SYSTEM")
	tmpSystemHivePath := filepath.Join(tmpDir, "SYSTEM")
	if err := fetchFileFromCim(GetCimPathFromLayer(layerPath), layerRelativeSystemHivePath, tmpSystemHivePath); err != nil {
		return err
	}

	if err := updateRegistryForCimBoot(tmpSystemHivePath); err != nil {
		return fmt.Errorf("failed to setup cim image for uvm boot: %s", err)
	}

	// Reopen the cim for writing
	cimWriter, err := create(GetCimDirFromLayer(layerPath), GetCimNameFromLayer(layerPath), "")
	if err != nil {
		return fmt.Errorf("failed to open cim at path %s: %s", layerPath, err)
	}
	defer func() {
		if err2 := cimWriter.close(); err2 != nil {
			if err == nil {
				err = err2
			}
		}
	}()

	if err := createBaseLayerHives(cimWriter); err != nil {
		return err
	}

	// add the layout file generated during processBaseLayer inside the cim.
	if err := cimWriter.addFileFromPath(layoutFileName, filepath.Join(layerPath, layoutFileName), []byte{}, []byte{}, []byte{}); err != nil {
		return fmt.Errorf("failed while adding layout file to cim: %s", err)
	}

	// add the BCD file updated during processBaseLayer inside the cim.
	if err := cimWriter.addFileFromPath(bcdFilePath, filepath.Join(layerPath, bcdFilePath), []byte{}, []byte{}, []byte{}); err != nil {
		return fmt.Errorf("failed while adding BCD file to cim: %s", err)
	}

	// This MUST come after createBaselayerHives otherwise createBaseLayerHives will overwrite the
	// changed system hive file.
	if err := cimWriter.addFileFromPath(layerRelativeSystemHivePath, tmpSystemHivePath, []byte{}, []byte{}, []byte{}); err != nil {
		return fmt.Errorf("failed while updating SYSTEM registry inside cim: %s", err)
	}

	if err := debuggingSetup(cimWriter); err != nil {
		return fmt.Errorf("failed during debugging setup: %s", err)
	}
	return nil
}

// debuggingSetup can be used to do any kind of debugging related operation on the cim
// before it is closed. Mostly this is used to replace some files inside the cim.
func debuggingSetup(cimWriter *cimFsWriter) error {
	// Overwrite the wcifs.sys & cimfs.sys files inside the cim.
	overwriteFiles := []struct {
		hostPath string // File on the host that should be added to cim
		cimPath  string // Path inside the cim.
	}{
		{"D:\\cimfs\\cimfs.sys", "UtilityVM\\Files\\Windows\\System32\\drivers\\cimfs.sys"},
		{"D:\\cimfs\\wcifs.sys", "UtilityVM\\Files\\Windows\\System32\\drivers\\wcifs.sys"},
	}
	for _, replace := range overwriteFiles {
		if err := cimWriter.addFileFromPath(replace.cimPath, replace.hostPath, []byte{}, []byte{}, []byte{}); err != nil {
			return err
		}
	}
	return nil
}
