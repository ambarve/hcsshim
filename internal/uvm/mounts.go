package uvm

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/Microsoft/go-winio/pkg/guid"
	"github.com/Microsoft/hcsshim/internal/guestrequest"
	"github.com/Microsoft/hcsshim/internal/requesttype"
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
	// Unique GUID assigned to a cim.
	cimID guid.GUID
	// ref count for number of times this cim was mounted.
	refCount uint32
}

// Adds a cim located at hostCimPath (i.e inside the host filesystem) into the UVM as a vsmb share and then
// mounts that cim inside the uvm. Returns the mount location of the cim inside the uvm.
func (uvm *UtilityVM) MountInUVM(ctx context.Context, hostCimPath string) (_ string, err error) {
	if !strings.HasSuffix(hostCimPath, ".cim") {
		return "", fmt.Errorf("invalid cim file path: %s", hostCimPath)
	}
	if _, ok := uvm.cimMounts[hostCimPath]; !ok {
		layerGUID, err := guid.NewV4()
		if err != nil {
			return "", fmt.Errorf("error creating guid: %s", err)
		}
		// Always add the parent directory of the cim as a vsmb mount because
		// there are region files in that directory that also should be shared
		// in the uvm
		hostCimDir := filepath.Dir(hostCimPath)
		// Add the VSMB share
		options := uvm.DefaultVSMBOptions(true)
		options.NoDirectmap = false
		if _, err := uvm.AddVSMB(ctx, hostCimDir, options); err != nil {
			return "", fmt.Errorf("failed while adding vsmb share for cim: %s", err)
		}
		defer func() {
			if err != nil {
				uvm.RemoveVSMB(ctx, hostCimDir, true)
			}
		}()
		// get path for that share
		uvmPath, err := uvm.GetVSMBUvmPath(ctx, hostCimDir, true)
		if err != nil {
			return "", fmt.Errorf("failed to get vsmb uvm path while mounting cim: %s", err)
		}

		guestReq := guestrequest.GuestRequest{
			ResourceType: guestrequest.ResourceTypeCimMount,
			RequestType:  requesttype.Add,
			Settings: &hcsschema.CimMount{
				ImagePath:      uvmPath,
				FileSystemName: filepath.Base(hostCimPath),
				VolumeGuid:     layerGUID.String(),
				MountFlags:     hcsschema.CimMountFlagEnableDax | hcsschema.CimMountFlagCacheFiles,
			},
		}
		if err := uvm.GuestRequest(ctx, guestReq); err != nil {
			return "", fmt.Errorf("failed to mount the cim: %s", err)
		}

		uvm.cimMounts[hostCimPath] = &cimInfo{layerGUID, 0}
	}
	ci := uvm.cimMounts[hostCimPath]
	ci.refCount += 1
	return fmt.Sprintf("\\\\?\\Volume{%s}", ci.cimID), nil
}

// Returns the path ("\\?\Volume{GUID}\" format) at which the cim at hostCimPath is mounted
// inside the uvm.
// Throws an error if the given cim is not mounted.
func (uvm *UtilityVM) GetCimUvmMountPathNt(hostCimPath string) (string, error) {
	ci, ok := uvm.cimMounts[hostCimPath]
	if !ok {
		return "", fmt.Errorf("cim %s is not mounted", hostCimPath)
	}
	return fmt.Sprintf("\\\\?\\Volume{%s}\\", ci.cimID), nil
}

// If the cim located at the `hostCimPath` is mounted inside the given uvm then unmount that cim,
// removes the vsmb share associated with if this is the last reference to that mounted cim.
func (uvm *UtilityVM) UnMountFromUVM(ctx context.Context, hostCimPath string) error {
	ci, ok := uvm.cimMounts[hostCimPath]
	if !ok {
		return fmt.Errorf("cim not mounted inside the uvm")
	}
	// get path for that share
	uvmPath, err := uvm.GetVSMBUvmPath(ctx, hostCimPath, true)
	if err != nil {
		return fmt.Errorf("failed to get vsmb uvm path while mounting cim: %s", err)
	}
	if ci.refCount == 1 {
		guestReq := guestrequest.GuestRequest{
			ResourceType: guestrequest.ResourceTypeCimMount,
			RequestType:  requesttype.Remove,
			Settings: &hcsschema.CimMount{
				ImagePath:      uvmPath,
				FileSystemName: filepath.Base(hostCimPath),
				VolumeGuid:     ci.cimID.String(),
			},
		}
		if err := uvm.GuestRequest(ctx, guestReq); err != nil {
			return fmt.Errorf("failed to mount the cim: %s", err)
		}
		delete(uvm.cimMounts, hostCimPath)
	} else {
		ci.refCount -= 1
	}
	return nil
}
