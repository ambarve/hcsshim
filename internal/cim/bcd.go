package cim

import (
	"bytes"
	"fmt"
	"os/exec"
	"path/filepath"

	"github.com/Microsoft/go-winio/pkg/guid"
)

func bcdExec(storePath string, args ...string) error {
	var out bytes.Buffer
	argsArr := []string{"/store", storePath, "/offline"}
	argsArr = append(argsArr, args...)
	cmd := exec.Command("bcdedit.exe", argsArr...)
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		fmt.Errorf("bcd command (%s) failed: %s", cmd, err)
	}
	return nil
}

func setBcdRestartOnFailure(storePath string) error {
	return bcdExec(storePath, "/set", "{default}", "restartonfailure", "yes")
}

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

func setBcdOsArcDevice(storePath string, diskID, partitionID guid.GUID) error {
	return bcdExec(storePath, "/set", "{default}", "osarcdevice", fmt.Sprintf("gpt_partition={%s};{%s}", diskID, partitionID))
}

// Updates the bcd store at path `layerPath + "Files\\EFI\\Microsoft\\Boot\\BCD" to boot with the
// disk with given ID and given partitionID.
func UpdateBcdStoreForBoot(layerPath string, diskID, partitionID guid.GUID) error {
	storePath := filepath.Join(layerPath, "Files\\EFI\\Microsoft\\Boot\\BCD")
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

// Only added to help with debugging the uvm
func setDebugOn(storePath string) error {
	return bcdExec(storePath, "/set", "{default}", "debug", "on")
}
