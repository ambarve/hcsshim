package fs

import (
	"fmt"
	"path/filepath"

	"github.com/Microsoft/go-winio/pkg/guid"
	"github.com/Microsoft/hcsshim/internal/mylogger"
	hcsschema "github.com/Microsoft/hcsshim/internal/schema2"
)

type MountError struct {
	Cim        string
	Op         string
	VolumeGUID guid.GUID
	Err        error
}

func (e *MountError) Error() string {
	s := "cim " + e.Op
	if e.Cim != "" {
		s += " " + e.Cim
	}
	s += " " + e.VolumeGUID.String() + ": " + e.Err.Error()
	return s
}

type cimInfo struct {
	// path to the cim
	path string
	// Unique GUID assigned to a cim.
	cimID guid.GUID
	// ref count for number of times this cim was mounted.
	refCount uint32
}

// map for information about cims mounted on the host
var hostCimMounts = make(map[string]*cimInfo)

// Mount mounts the cim at path `cimPath` and returns the mount location of that cim.
// If this cim is already mounted then nothing is done.
func Mount(cimPath string) (string, error) {
	if _, ok := hostCimMounts[cimPath]; !ok {
		layerGUID, err := guid.NewV4()
		if err != nil {
			return "", fmt.Errorf("error creating guid: %s", err)
		}
		if err := cimMountImage(filepath.Dir(cimPath), filepath.Base(cimPath), hcsschema.CimMountFlagCacheFiles, &layerGUID); err != nil {
			return "", &MountError{Cim: cimPath, Op: "Mount", VolumeGUID: layerGUID, Err: err}
		}
		hostCimMounts[cimPath] = &cimInfo{cimPath, layerGUID, 0}
	}
	ci := hostCimMounts[cimPath]
	ci.refCount += 1
	mylogger.LogFmt("Mount cim: %s, refCount: %d, mounted ID: %s\n", cimPath, ci.refCount, ci.cimID)
	return fmt.Sprintf("\\\\?\\Volume{%s}", ci.cimID), nil
}

// Returns the path ("\\?\Volume{GUID}" format) at which the cim with given cimPath is mounted
// Throws an error if the given cim is not mounted.
func GetCimMountPath(cimPath string) (string, error) {
	ci, ok := hostCimMounts[cimPath]
	if !ok {
		return "", fmt.Errorf("cim %s is not mounted", cimPath)
	}
	return fmt.Sprintf("\\\\?\\Volume{%s}", ci.cimID), nil
}

// UnMount unmounts the cim at path `cimPath` if this is the last reference to it.
func UnMount(cimPath string) error {
	ci, ok := hostCimMounts[cimPath]
	if !ok {
		return fmt.Errorf("cim not mounted")
	}
	if ci.refCount == 1 {
		if err := cimDismountImage(&ci.cimID); err != nil {
			return fmt.Errorf("error dismounting the cim: %s", err)
		}
		delete(hostCimMounts, cimPath)
	} else {
		ci.refCount -= 1
	}
	return nil
}
