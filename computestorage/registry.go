package computestorage

import (
	"bytes"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"

	"github.com/Microsoft/go-winio/pkg/guid"
	"github.com/Microsoft/hcsshim/internal/mylogger"
	"github.com/pkg/errors"
)

func bcdExec(storePath string, args ...string) error {
	var out bytes.Buffer
	argsArr := []string{"/store", storePath, "/offline"}
	argsArr = append(argsArr, args...)
	cmd := exec.Command("bcdedit.exe", argsArr...)
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("bcd command (%s) failed: %s, stdout: %s", cmd, err, out.String())
	}
	return nil
}

func execWithPowershell(args ...string) error {
	var out bytes.Buffer
	cmd := exec.Command("powershell.exe", args...)
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		fmt.Printf("execWithPowershell (%s) failed with error: %s, stdout: %s\n", cmd.String(), err, out.String())
		return err
	}
	fmt.Println(out.String())
	return nil
}

// A registry configuration required for the uvm.
func setBcdRestartOnFailure(storePath string) error {
	return bcdExec(storePath, "/set", "{default}", "restartonfailure", "yes")
}

// A registry configuration required for the uvm.
func setBcdCimfsBootDevice(storePath string, drivePath string) error {
	// vmbusDeviceStr := "vmbus={c63c9bdf-5fa5-4208-b03f-6b458b365592}"
	// vmbusDeviceStr := fmt.Sprintf("hd_cimfs={1b17b234-911f-4cab-8c42-3fa994dc4b4f};z:,{763e9fea-502d-434f-aad9-5fabe9c91a7b}", drivePath)
	vmbusDeviceStr := "hd_cimfs={1b17b234-911f-4cab-8c42-3fa994dc4b4f};F:,{763e9fea-502d-434f-aad9-5fabe9c91a7b}"

	// mountvol()
	// fmt.Printf("wait for you to check on volume\n")
	// time.Sleep(20 * time.Second)

	if err := bcdExec(storePath, "/set", "{default}", "device", vmbusDeviceStr); err != nil {
		return err
	}

	if err := bcdExec(storePath, "/set", "{default}", "osdevice", vmbusDeviceStr); err != nil {
		return err
	}

	if err := bcdExec(storePath, "/create", "{763e9fea-502d-434f-aad9-5fabe9c91a7b}", "/d", "CIMFS device options", "/device"); err != nil {
		return err
	}

	// cimdirpath := filepath.Join(filepath.Dir(filepath.Join(filepath.Dir(storePath), "..\\..\\..\\..\\..\\")), "cim-layers")
	cimdirpath := "\\cim-layers"
	// mylogger.LogFmt("setting cimdir path to: %s", cimdirpath)
	if err := bcdExec(storePath, "/set", "{763e9fea-502d-434f-aad9-5fabe9c91a7b}", "cimfsrootdirectory", cimdirpath); err != nil {
		return err
	}

	// change
	if err := bcdExec(storePath, "/set", "{default}", "path", "\\UtilityVM\\Files\\Windows\\System32\\boot\\winload.efi"); err != nil {
		return err
	}

	if err := bcdExec(storePath, "/set", "{default}", "systemroot", "\\UtilityVM\\Files\\Windows"); err != nil {
		return err
	}

	return nil
}

// A registry configuration required for the uvm.
func setBcdOsArcDevice(storePath string, diskID, partitionID guid.GUID) error {
	return bcdExec(storePath, "/set", "{default}", "osarcdevice", fmt.Sprintf("gpt_partition={%s};{%s}", diskID, partitionID))
}

// // updateBcdStoreForBoot Updates the bcd store at path `storePath` to boot with the disk
// // with given ID and given partitionID.
// func updateBcdStoreForBoot(storePath string, diskID, partitionID guid.GUID) error {
// 	if err := setBcdRestartOnFailure(storePath); err != nil {
// 		return err
// 	}

// 	if err := setBcdVmbusBootDevice(storePath); err != nil {
// 		return err
// 	}

// 	if err := setBcdOsArcDevice(storePath, diskID, partitionID); err != nil {
// 		return err
// 	}
// 	return setDebugOn(storePath)
// 	// return nil
// }

// Mount volumePath (in format '\\?\Volume{GUID}' at targetPath.
// https://docs.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-setvolumemountpointw
func setVolumeMountPoint(targetPath string, volumePath string) error {
	if !strings.HasPrefix(volumePath, "\\\\?\\Volume{") {
		return errors.Errorf("unable to mount non-volume path %s", volumePath)
	}

	// Both must end in a backslash
	// slashedTarget := filepath.Clean(targetPath) + string(filepath.Separator)
	slashedTarget := targetPath
	mylogger.LogFmt("cleaned target path: %s, slashed target: %s\n", filepath.Clean(targetPath), slashedTarget)
	slashedVolume := volumePath + string(filepath.Separator)

	targetP, err := windows.UTF16PtrFromString(slashedTarget)
	if err != nil {
		return errors.Wrapf(err, "unable to utf16-ise %s", slashedTarget)
	}

	volumeP, err := windows.UTF16PtrFromString(slashedVolume)
	if err != nil {
		return errors.Wrapf(err, "unable to utf16-ise %s", slashedVolume)
	}

	if err := windows.SetVolumeMountPoint(targetP, volumeP); err != nil {
		return errors.Wrapf(err, "failed calling SetVolumeMount('%s', '%s')", slashedTarget, slashedVolume)
	}

	return nil
}

// Remove the volume mount at targetPath
// https://docs.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-deletevolumemountpointa
func deleteVolumeMountPoint(targetPath string) error {
	// Must end in a backslash
	slashedTarget := filepath.Clean(targetPath) + string(filepath.Separator)

	targetP, err := windows.UTF16PtrFromString(slashedTarget)
	if err != nil {
		return errors.Wrapf(err, "unable to utf16-ise %s", slashedTarget)
	}

	if err := windows.DeleteVolumeMountPoint(targetP); err != nil {
		return errors.Wrapf(err, "failed calling DeleteVolumeMountPoint('%s')", slashedTarget)
	}

	return nil
}

// updateBcdStoreForFullCimBoot Updates the bcd store at path `storePath` to boot with the disk
// with given ID and given partitionID.
func updateBcdStoreForFullCimBoot(storePath string, diskID, partitionID guid.GUID) (err error) {
	// vhd which is going to have the cim
	cimVhdPath := "D:\\Containers\\testdata\\cimboot\\sandbox.vhdx"

	if err = execWithPowershell("Mount-VHD", cimVhdPath); err != nil {
		return fmt.Errorf("mount vhd failed : %s", err)
	}
	defer execWithPowershell("Dismount-VHD", cimVhdPath)

	// err = wclayer.ActivateLayer(context.TODO(), filepath.Dir(cimVhdPath))
	// if err != nil {
	// 	return errors.Wrap(err, "failed to activate layer during BCD setup for full cim boot")
	// }
	// defer wclayer.DeactivateLayer(context.TODO(), filepath.Dir(cimVhdPath))

	// mountPath, err := wclayer.GetLayerMountPath(context.TODO(), filepath.Dir(cimVhdPath))
	// if err != nil {
	// 	return errors.Wrap(err, "failed to get layer mount path during BCD setup for full cim boot")
	// }

	// mylogger.LogFmt("mount Path is %s\n", mountPath)
	targetPath := "F:"

	// if err = setVolumeMountPoint(targetPath+"\\", mountPath); err != nil {
	// 	return err
	// }
	// defer func() {
	// 	deleteVolumeMountPoint(targetPath)
	// }()

	// if err := setBcdRestartOnFailure(storePath); err != nil {
	// 	return err
	// }

	if err := setBcdCimfsBootDevice(storePath, targetPath); err != nil {
		return err
	}

	if err := setBcdOsArcDevice(storePath, diskID, partitionID); err != nil {
		return err
	}
	return setDebugOn(storePath)
	// return nil
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
