//go:build windows

package cim

import (
	"fmt"

	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/Microsoft/hcsshim/internal/winapi"
	"github.com/Microsoft/hcsshim/osversion"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// mergeHive merges the hive located at parentHivePath with the hive located at deltaHivePath and stores
// the result into the file at mergedHivePath. If a file already exists at path `mergedHivePath` then it
// throws an error.
func mergeHive(parentHivePath, deltaHivePath, mergedHivePath string) (err error) {
	var baseHive, deltaHive, mergedHive winapi.ORHKey
	if err := winapi.OROpenHive(parentHivePath, &baseHive); err != nil {
		return fmt.Errorf("failed to open base hive %s: %w", parentHivePath, err)
	}
	defer func() {
		err2 := winapi.ORCloseHive(baseHive)
		if err == nil {
			err = errors.Wrap(err2, "failed to close base hive")
		}
	}()
	if err := winapi.OROpenHive(deltaHivePath, &deltaHive); err != nil {
		return fmt.Errorf("failed to open delta hive %s: %w", deltaHivePath, err)
	}
	defer func() {
		err2 := winapi.ORCloseHive(deltaHive)
		if err == nil {
			err = errors.Wrap(err2, "failed to close delta hive")
		}
	}()
	if err := winapi.ORMergeHives([]winapi.ORHKey{baseHive, deltaHive}, &mergedHive); err != nil {
		return fmt.Errorf("failed to merge hives: %w", err)
	}
	defer func() {
		err2 := winapi.ORCloseHive(mergedHive)
		if err == nil {
			err = errors.Wrap(err2, "failed to close merged hive")
		}
	}()
	if err := winapi.ORSaveHive(mergedHive, mergedHivePath, uint32(osversion.Get().MajorVersion), uint32(osversion.Get().MinorVersion)); err != nil {
		return fmt.Errorf("failed to save hive: %w", err)
	}
	return
}

// getOsBuildNumberFromRegistry fetches the "CurrentBuild" value at path
// "Microsoft\Windows NT\CurrentVersion" from the SOFTWARE registry hive at path
// `regHivePath`. This is used to detect the build version of the uvm.
func getOsBuildNumberFromRegistry(regHivePath string) (_ string, err error) {
	var storeHandle, keyHandle winapi.ORHKey
	var dataType, dataLen uint32
	keyPath := "Microsoft\\Windows NT\\CurrentVersion"
	valueName := "CurrentBuild"
	dataLen = 16 // build version string can't be more than 5 wide chars?
	dataBuf := make([]byte, dataLen)

	if err = winapi.OROpenHive(regHivePath, &storeHandle); err != nil {
		return "", fmt.Errorf("failed to open registry store at %s: %s", regHivePath, err)
	}
	defer func() {
		if closeErr := winapi.ORCloseHive(storeHandle); closeErr != nil {
			log.L.WithFields(logrus.Fields{
				"error": closeErr,
				"hive":  regHivePath,
			}).Warnf("failed to close hive")
		}
	}()

	if err = winapi.OROpenKey(storeHandle, keyPath, &keyHandle); err != nil {
		return "", fmt.Errorf("failed to open key at %s: %s", keyPath, err)
	}
	defer func() {
		if closeErr := winapi.ORCloseKey(keyHandle); closeErr != nil {
			log.L.WithFields(logrus.Fields{
				"error": closeErr,
				"hive":  regHivePath,
				"key":   keyPath,
				"value": valueName,
			}).Warnf("failed to close hive key")
		}
	}()

	if err = winapi.ORGetValue(keyHandle, "", valueName, &dataType, &dataBuf[0], &dataLen); err != nil {
		return "", fmt.Errorf("failed to get value of %s: %s", valueName, err)
	}

	if dataType != uint32(winapi.REG_TYPE_SZ) {
		return "", fmt.Errorf("unexpected build number data type (%d)", dataType)
	}

	return winapi.ParseUtf16LE(dataBuf[:(dataLen - 2)]), nil
}
