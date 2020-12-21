package layer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"unsafe"

	"github.com/Microsoft/go-winio/pkg/guid"
	"github.com/Microsoft/hcsshim/internal/cim"
	"github.com/Microsoft/hcsshim/internal/winapi"
	"github.com/Microsoft/hcsshim/osversion"
	"github.com/pkg/errors"
	"golang.org/x/sys/windows"
)

func bcdExec(storePath string, args ...string) error {
	var out bytes.Buffer
	argsArr := []string{"/store", storePath, "/offline"}
	argsArr = append(argsArr, args...)
	cmd := exec.Command("bcdedit.exe", argsArr...)
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("bcd command (%s) failed: %s", cmd, err)
	}
	return nil
}

// A registry configuration required for the uvm.
func setBcdRestartOnFailure(storePath string) error {
	return bcdExec(storePath, "/set", "{default}", "restartonfailure", "yes")
}

// A registry configuration required for the uvm.
func setBcdVmbusBootDevice(storePath string) error {
	vmbusDeviceStr := "vmbus={c63c9bdf-5fa5-4208-b03f-6b458b365592}"
	if err := bcdExec(storePath, "/set", "{default}", "device", vmbusDeviceStr); err != nil {
		return err
	}

	if err := bcdExec(storePath, "/set", "{default}", "osdevice", vmbusDeviceStr); err != nil {
		return err
	}

	if err := bcdExec(storePath, "/set", "{bootmgr}", "alternatebootdevice", vmbusDeviceStr); err != nil {
		return err
	}
	return nil
}

// A registry configuration required for the uvm.
func setBcdOsArcDevice(storePath string, diskID, partitionID guid.GUID) error {
	return bcdExec(storePath, "/set", "{default}", "osarcdevice", fmt.Sprintf("gpt_partition={%s};{%s}", diskID, partitionID))
}

// updateBcdStoreForBoot Updates the bcd store at path `storePath` to boot with the disk
// with given ID and given partitionID.
func updateBcdStoreForBoot(storePath string, diskID, partitionID guid.GUID) error {
	if err := setBcdRestartOnFailure(storePath); err != nil {
		return err
	}

	if err := setBcdVmbusBootDevice(storePath); err != nil {
		return err
	}

	if err := setBcdOsArcDevice(storePath, diskID, partitionID); err != nil {
		return err
	}
	return setDebugOn(storePath)
}

// updateRegistryForCimBoot Opens the SYSTEM registry hive at path `hivePath` and updates
// it to enable uvm boot from the cim. We need to set following values in the SYSTEM
// registry:
// To ask the uvm to boot directly from the cim setup following registry keys:
// (TODO(ambarve): only supported for iron+ hosts)
// 1. ControlSet001\Control\HVSI /v WCIFSCIMFSContainerMode /t REG_DWORD /d 0x1
// 2. ControlSet001\Control\HVSI /v WCIFSContainerMode /t REG_DWORD /d 0x1
// 3. ControlSet001\Services\CimFS /v Start /t REG_DWORD /d 0x0
// To specify the path of the cim from which to boot set up following registry keys. Whenever a uvm
// is created a vsmb share is automatically added to that uvm to share the directory which holds all the cim
// files. This registry key should specify a path whose first element is the name of that share and the second
// element is the name of the cim.
// 1. ControlSet001\Control\HVSI /v CimRelativePath /t REG_SZ /d  $CimVsmbShareName`+\\+`$nameofthelayercim`
// Our cim includes files for both the uvm and the containers. All the files for the uvm are kept inside the
// `UtilityVM\Files` directory so below registry key specifies the name of this directory inside the cim which
// contains all the uvm related files.
// ControlSet001\Control\HVSI /v UvmLayerRelativePath /t REG_SZ /d UtilityVM\\Files\\ (the ending \\ is important)
func updateRegistryForCimBoot(layerPath, hivePath string) (err error) {
	dataZero := make([]byte, 4)
	dataOne := make([]byte, 4)
	binary.LittleEndian.PutUint32(dataOne, uint32(1))
	uvmLayerRelativePathData, err := windows.UTF16FromString("UtilityVM\\Files\\")
	if err != nil {
		return fmt.Errorf("can not convert string to utf16: %s", err)
	}
	cimRelativePath := cim.CimVsmbShareName + "\\" + cim.GetCimNameFromLayer(layerPath)
	cimRelativePathData, err := windows.UTF16FromString(cimRelativePath)
	if err != nil {
		return fmt.Errorf("can not convert string to utf16: %s", err)
	}

	regChanges := []struct {
		keyPath   string
		valueName string
		valueType winapi.RegType
		data      *byte
		dataLen   uint32
	}{
		{"ControlSet001\\Control\\HVSI", "WCIFSCIMFSContainerMode", winapi.REG_TYPE_DWORD, &dataOne[0], uint32(len(dataOne))},
		{"ControlSet001\\Control\\HVSI", "WCIFSContainerMode", winapi.REG_TYPE_DWORD, &dataOne[0], uint32(len(dataOne))},
		{"ControlSet001\\Control\\HVSI", "CimRelativePath", winapi.REG_TYPE_SZ, (*byte)(unsafe.Pointer(&cimRelativePathData[0])), uint32(2 * len(cimRelativePathData))},
		{"ControlSet001\\Control\\HVSI", "UvmLayerRelativePath", winapi.REG_TYPE_SZ, (*byte)(unsafe.Pointer(&uvmLayerRelativePathData[0])), uint32(2 * len(uvmLayerRelativePathData))},
		{"ControlSet001\\Services\\CimFS", "Start", winapi.REG_TYPE_DWORD, &dataZero[0], uint32(len(dataZero))},
	}

	var storeHandle winapi.OrHKey
	if err = winapi.OrOpenHive(hivePath, &storeHandle); err != nil {
		return fmt.Errorf("failed to open registry store at %s: %s", hivePath, err)
	}

	for _, change := range regChanges {
		var changeKey winapi.OrHKey
		if err = winapi.OrCreateKey(storeHandle, change.keyPath, 0, 0, 0, &changeKey, nil); err != nil {
			return fmt.Errorf("failed to open reg key %s: %s", change.keyPath, err)
		}

		if err = winapi.OrSetValue(changeKey, change.valueName, uint32(change.valueType), change.data, change.dataLen); err != nil {
			return fmt.Errorf("failed to set value for regkey %s\\%s : %s", change.keyPath, change.valueName, err)
		}
	}

	// remove the existing file first
	if err := os.Remove(hivePath); err != nil {
		return fmt.Errorf("failed to remove existing registry %s: %s", hivePath, err)
	}

	if err = winapi.OrSaveHive(winapi.OrHKey(storeHandle), hivePath, uint32(osversion.Get().MajorVersion), uint32(osversion.Get().MinorVersion)); err != nil {
		return fmt.Errorf("error saving the registry store: %s", err)
	}

	// close hive irrespective of the errors
	if err := winapi.OrCloseHive(winapi.OrHKey(storeHandle)); err != nil {
		return fmt.Errorf("error closing registry store; %s", err)
	}
	return nil

}

// Only added to help with debugging the uvm
func setDebugOn(storePath string) error {
	if err := bcdExec(storePath, "/set", "{default}", "testsigning", "on"); err != nil {
		return err
	}
	// if err := bcdExec(storePath, "/set", "{default}", "bootdebug", "on"); err != nil {
	// 	return err
	// }
	// if err := bcdExec(storePath, "/set", "{bootmgr}", "bootdebug", "on"); err != nil {
	// 	return err
	// }
	// if err := bcdExec(storePath, "/dbgsettings", "SERIAL", "DEBUGPORT:1", "BAUDRATE:115200"); err != nil {
	// 	return err
	// }
	// return bcdExec(storePath, "/set", "{default}", "debug", "on")
	return nil
}

// mergeWithParentLayerHives merges the delta hives of current layer with the base registry
// hives of its parent layer. This function reads the parent layer cim to fetch registry
// hives of the parent layer and reads the `layerPath\\Hives` directory to read the hives
// form the current layer. The merged hives are stored in the directory provided by
// `outputDir`
func mergeWithParentLayerHives(layerPath, parentLayerPath, outputDir string) error {
	// create a temp directory to store parent layer hive files
	tmpParentLayer, err := ioutil.TempDir("", "")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %s", tmpParentLayer)
	}
	defer os.RemoveAll(tmpParentLayer)

	parentCimPath := cim.GetCimPathFromLayer(parentLayerPath)

	for _, hv := range hives {
		err := cim.FetchFileFromCim(parentCimPath, filepath.Join(hivesPath, hv.base), filepath.Join(tmpParentLayer, hv.base))
		if err != nil {
			return err
		}
	}

	// merge hives
	for _, hv := range hives {
		err := mergeHive(filepath.Join(tmpParentLayer, hv.base), filepath.Join(layerPath, hivesPath, hv.delta), filepath.Join(outputDir, hv.base))
		if err != nil {
			return err
		}
	}
	return nil
}

// mergeHive merges the hive located at parentHivePath with the hive located at deltaHivePath and stores
// the result into the file at mergedHivePath. If a file already exists at path `mergedHivePath` then it
// throws an error.
func mergeHive(parentHivePath, deltaHivePath, mergedHivePath string) (err error) {
	var baseHive, deltaHive, mergedHive winapi.OrHKey
	if err := winapi.OrOpenHive(parentHivePath, &baseHive); err != nil {
		return fmt.Errorf("failed to open base hive %s: %s", parentHivePath, err)
	}
	defer func() {
		err2 := winapi.OrCloseHive(baseHive)
		if err == nil {
			err = errors.Wrap(err2, "failed to close base hive")
		}
	}()
	if err := winapi.OrOpenHive(deltaHivePath, &deltaHive); err != nil {
		return fmt.Errorf("failed to open delta hive %s: %s", deltaHivePath, err)
	}
	defer func() {
		err2 := winapi.OrCloseHive(deltaHive)
		if err == nil {
			err = errors.Wrap(err2, "failed to close delta hive")
		}
	}()
	if err := winapi.OrMergeHives([]winapi.OrHKey{baseHive, deltaHive}, &mergedHive); err != nil {
		return fmt.Errorf("failed to merge hives: %s", err)
	}
	defer func() {
		err2 := winapi.OrCloseHive(mergedHive)
		if err == nil {
			err = errors.Wrap(err2, "failed to close merged hive")
		}
	}()
	if err := winapi.OrSaveHive(mergedHive, mergedHivePath, uint32(osversion.Get().MajorVersion), uint32(osversion.Get().MinorVersion)); err != nil {
		return fmt.Errorf("failed to save hive: %s", err)
	}
	return
}
