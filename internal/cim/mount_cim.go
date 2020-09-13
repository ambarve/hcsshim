package cim

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/Microsoft/go-winio/pkg/guid"
	"github.com/Microsoft/hcsshim/internal/guestrequest"
	"github.com/Microsoft/hcsshim/internal/requesttype"
	hcsschema "github.com/Microsoft/hcsshim/internal/schema2"
	"github.com/Microsoft/hcsshim/internal/uvm"
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

var (
	// map for information about cims mounted on the host
	hostCimMounts = make(map[string]*cimInfo)
	// map for information about cims mounted on the uvm
	uvmCimMounts = make(map[string]*cimInfo)
)

// Mount mounts the cim at path `cimPath` and returns the mount location of that cim.
// If this cim is already mounted then nothing is done.
func Mount(cimPath string) (string, error) {
	if _, ok := hostCimMounts[cimPath]; !ok {
		layerGUID, err := guid.NewV4()
		if err != nil {
			return "", fmt.Errorf("error creating guid: %s", err)
		}
		if err := cimMountImage(filepath.Dir(cimPath), filepath.Base(cimPath), 0, &layerGUID); err != nil {
			return "", &MountError{Cim: cimPath, Op: "Mount", VolumeGUID: layerGUID, Err: err}
		}
		hostCimMounts[cimPath] = &cimInfo{layerGUID, 0}
	}
	ci := hostCimMounts[cimPath]
	ci.refCount += 1
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

// Adds a cim located at hostCimPath (i.e inside the host filesystem) into the UVM as a vsmb share and then
// mounts that cim inside the uvm. Returns the mount location of the cim inside the uvm.
func MountInUVM(ctx context.Context, uvm *uvm.UtilityVM, hostCimPath string) (_ string, err error) {
	if _, ok := uvmCimMounts[hostCimPath]; !ok {
		layerGUID, err := guid.NewV4()
		if err != nil {
			return "", fmt.Errorf("error creating guid: %s", err)
		}
		// Add the VSMB share
		options := uvm.DefaultVSMBOptions(true)
		if _, err := uvm.AddVSMB(ctx, hostCimPath, options); err != nil {
			return "", fmt.Errorf("failed while adding vsmb share for cim: %s", err)
		}
		defer func() {
			if err != nil {
				uvm.RemoveVSMB(ctx, hostCimPath, true)
			}
		}()
		// get path for that share
		uvmPath, err := uvm.GetVSMBUvmPath(ctx, hostCimPath, true)
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
			},
		}
		if err := uvm.GuestRequest(ctx, guestReq); err != nil {
			return "", fmt.Errorf("failed to mount the cim: %s", err)
		}
		uvmCimMounts[hostCimPath] = &cimInfo{layerGUID, 0}
	}
	ci := uvmCimMounts[hostCimPath]
	ci.refCount += 1
	return fmt.Sprintf("\\\\?\\Volume{%s}", ci.cimID), nil
}

// Returns the path ("\\?\Volume{GUID}" format) at which the cim with cim at hostCimPath is mounted
// inside the uvm.
// Throws an error if the given cim is not mounted.
func GetCimUvmMountPath(hostCimPath string) (string, error) {
	ci, ok := uvmCimMounts[hostCimPath]
	if !ok {
		return "", fmt.Errorf("cim %s is not mounted", hostCimPath)
	}
	return fmt.Sprintf("\\\\?\\Volume{%s}", ci.cimID), nil
}

// If the cim located at the `hostCimPath` is mounted inside the given uvm then unmount that cim,
// removes the vsmb share associated with if this is the last reference to that mounted cim.
func UnMountFromUVM(ctx context.Context, uvm *uvm.UtilityVM, hostCimPath string) error {
	ci, ok := uvmCimMounts[hostCimPath]
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
		delete(uvmCimMounts, hostCimPath)
	} else {
		ci.refCount -= 1
	}
	return nil
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
