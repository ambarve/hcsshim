package cim

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"unsafe"

	"github.com/Microsoft/go-winio/pkg/guid"
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

	return nil
}

// updateRegistryForCimBoot Opens the SYSTEM registry hive at path `hivePath` and updates
// it to enable uvm boot from the cim. We need to set following values in the SYSTEM
// registry:
// ControlSet001\Control\HVSI /v WCIFSCIMFSContainerMode /t REG_DWORD /d 0x1
// ControlSet001\Control\HVSI /v WCIFSContainerMode /t REG_DWORD /d 0x1
// ControlSet001\Services\CimFS /v Start /t REG_DWORD /d 0x0
// ControlSet001\Control\HVSI /v RelatvieBootPath /t REG_SZ /d UtilityVM\\Files\\ (the ending \\ is important)
func updateRegistryForCimBoot(hivePath string) (err error) {
	dataZero := make([]byte, 4)
	dataOne := make([]byte, 4)
	binary.LittleEndian.PutUint32(dataOne, uint32(1))
	relativePathData, err := windows.UTF16FromString("UtilityVM\\Files\\")
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
		{"ControlSet001\\Control\\HVSI", "RelativeBootPath", winapi.REG_TYPE_SZ, (*byte)(unsafe.Pointer(&relativePathData[0])), uint32(2 * len(relativePathData))},
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
	if err := bcdExec(storePath, "/set", "{default}", "bootdebug", "on"); err != nil {
		return err
	}
	if err := bcdExec(storePath, "/set", "{bootmgr}", "bootdebug", "on"); err != nil {
		return err
	}
	if err := bcdExec(storePath, "/dbgsettings", "SERIAL", "DEBUGPORT:1", "BAUDRATE:115200"); err != nil {
		return err
	}
	return bcdExec(storePath, "/set", "{default}", "debug", "on")
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
