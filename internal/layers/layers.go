//go:build windows
// +build windows

package layers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/windows"

	"github.com/Microsoft/hcsshim/internal/guestpath"
	hcsschema "github.com/Microsoft/hcsshim/internal/hcs/schema2"
	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/Microsoft/hcsshim/internal/ospath"
	"github.com/Microsoft/hcsshim/internal/uvm"
	"github.com/Microsoft/hcsshim/internal/wclayer"
)

// ImageLayers contains all the layers for an image.
type ImageLayers struct {
	vm                 *uvm.UtilityVM
	containerRootInUVM string
	volumeMountPath    string
	layers             []string
	// In some instances we may want to avoid cleaning up the image layers, such as when tearing
	// down a sandbox container since the UVM will be torn down shortly after and the resources
	// can be cleaned up on the host.
	skipCleanup bool
}

func NewImageLayers(vm *uvm.UtilityVM, containerRootInUVM string, layers []string, volumeMountPath string, skipCleanup bool) *ImageLayers {
	return &ImageLayers{
		vm:                 vm,
		containerRootInUVM: containerRootInUVM,
		layers:             layers,
		volumeMountPath:    volumeMountPath,
		skipCleanup:        skipCleanup,
	}
}

// Release unmounts all of the layers located in the layers array.
func (layers *ImageLayers) Release(ctx context.Context, all bool) error {
	if layers.skipCleanup && layers.vm != nil {
		return nil
	}
	op := UnmountOperationSCSI
	if layers.vm == nil || all {
		op = UnmountOperationAll
	}
	var crp string
	if layers.vm != nil {
		crp = containerRootfsPath(layers.vm, layers.containerRootInUVM)
	}
	err := UnmountContainerLayers(ctx, layers.layers, crp, layers.volumeMountPath, layers.vm, op)
	if err != nil {
		return err
	}
	layers.layers = nil
	return nil
}

// UnmountOperation is used when calling Unmount() to determine what type of unmount is
// required. In V1 schema, this must be unmountOperationAll. In V2, client can
// be more optimal and only unmount what they need which can be a minor performance
// improvement (eg if you know only one container is running in a utility VM, and
// the UVM is about to be torn down, there's no need to unmount the VSMB shares,
// just SCSI to have a consistent file system).
type UnmountOperation uint

const (
	UnmountOperationSCSI  UnmountOperation = 0x01
	UnmountOperationVSMB                   = 0x02
	UnmountOperationVPMEM                  = 0x04
	UnmountOperationAll                    = UnmountOperationSCSI | UnmountOperationVSMB | UnmountOperationVPMEM
)

// UnmountContainerLayers is a helper for clients to hide all the complexity of layer unmounting
func UnmountContainerLayers(ctx context.Context, layerFolders []string, containerRootPath, volumeMountPath string, vm *uvm.UtilityVM, op UnmountOperation) error {
	log.G(ctx).WithField("layerFolders", layerFolders).Debug("hcsshim::unmountContainerLayers")
	if vm == nil {
		// Must be an argon - folders are mounted on the host
		if op != UnmountOperationAll {
			return errors.New("only operation supported for host-mounted folders is unmountOperationAll")
		}
		if len(layerFolders) < 1 {
			return errors.New("need at least one layer for Unmount")
		}

		// Remove the mount point if there is one. This is the fallback case for job containers if no
		// bind mount support is available.
		if volumeMountPath != "" {
			if err := RemoveSandboxMountPoint(ctx, volumeMountPath); err != nil {
				return err
			}
		}

		path := layerFolders[len(layerFolders)-1]
		if err := wclayer.UnprepareLayer(ctx, path); err != nil {
			return err
		}
		if err := wclayer.DeactivateLayer(ctx, path); err != nil {
			return err
		}
		return nil
	}

	// V2 Xenon

	// Base+Scratch as a minimum. This is different to v1 which only requires the scratch
	if len(layerFolders) < 2 {
		return errors.New("at least two layers are required for unmount")
	}

	var retError error

	// Always remove the combined layers as they are part of scsi/vsmb/vpmem
	// removals.
	if vm.OS() == "windows" {
		if err := vm.RemoveCombinedLayersWCOW(ctx, containerRootPath); err != nil {
			log.G(ctx).WithError(err).Warn("failed guest request to remove combined layers")
			retError = err
		}
	} else {
		if err := vm.RemoveCombinedLayersLCOW(ctx, containerRootPath); err != nil {
			log.G(ctx).WithError(err).Warn("failed guest request to remove combined layers")
			retError = err
		}
	}

	// Unload the SCSI scratch path
	if (op & UnmountOperationSCSI) == UnmountOperationSCSI {
		hostScratchFile, err := getScratchVHDPath(layerFolders)
		if err != nil {
			return errors.Wrap(err, "failed to get scratch VHD path in layer folders")
		}
		if err := vm.RemoveSCSI(ctx, hostScratchFile); err != nil {
			log.G(ctx).WithError(err).Warn("failed to remove scratch")
			if retError == nil {
				retError = err
			} else {
				retError = errors.Wrapf(retError, err.Error())
			}
		}
	}

	// Remove each of the read-only layers from VSMB. These's are ref-counted and
	// only removed once the count drops to zero. This allows multiple containers
	// to share layers.
	if vm.OS() == "windows" && (op&UnmountOperationVSMB) == UnmountOperationVSMB {
		if e := unmountXenonWcowLayers(ctx, layerFolders, vm); e != nil {
			if retError == nil {
				retError = e
			} else {
				retError = errors.Wrapf(retError, e.Error())
			}
		}
	}

	// Remove each of the read-only layers from VPMEM (or SCSI). These's are ref-counted
	// and only removed once the count drops to zero. This allows multiple containers to
	// share layers. Note that SCSI is used on large layers.
	if vm.OS() == "linux" && (op&UnmountOperationVPMEM) == UnmountOperationVPMEM {
		for _, layerPath := range layerFolders[:len(layerFolders)-1] {
			hostPath := filepath.Join(layerPath, "layer.vhd")
			if err := removeLCOWLayer(ctx, vm, hostPath); err != nil {
				log.G(ctx).WithError(err).Warn("remove layer failed")
				if retError == nil {
					retError = err
				} else {
					retError = errors.Wrapf(retError, err.Error())
				}
			}
		}
	}

	return retError
}

// GetHCSLayers converts host paths corresponding to container layers into HCS schema V2 layers
func GetHCSLayers(ctx context.Context, vm *uvm.UtilityVM, paths []string) (layers []hcsschema.Layer, err error) {
	for _, path := range paths {
		uvmPath, err := vm.GetVSMBUvmPath(ctx, path, true)
		if err != nil {
			return nil, err
		}
		layerID, err := wclayer.LayerID(ctx, path)
		if err != nil {
			return nil, err
		}
		layers = append(layers, hcsschema.Layer{Id: layerID.String(), Path: uvmPath})
	}
	return layers, nil
}

// GetCimHCSLayer finds the uvm mount path of the given cim and returns a hcs schema v2
// layer of it.  The cim must have already been mounted inside the uvm.
func GetCimHCSLayer(ctx context.Context, vm *uvm.UtilityVM, cimPath, cimMountLocation string) (layers []hcsschema.Layer, err error) {
	var uvmPath string
	if vm.MountCimSupported() {
		hostCimDir := filepath.Dir(cimPath)
		uvmCimDir, err := vm.GetVSMBUvmPath(ctx, hostCimDir, true)
		if err != nil {
			return nil, fmt.Errorf("failed to get vsmb uvm path: %s", err)
		}
		uvmPath, err = vm.GetCimUvmMountPathNt(filepath.Join(uvmCimDir, filepath.Base(cimPath)))
		if err != nil {
			return nil, err
		}
	} else {
		uvmPath, err = vm.GetVSMBUvmPath(ctx, cimMountLocation, true)
		if err != nil {
			return nil, err
		}
	}
	// Note: the LayerID must still be calculated with the cim path. The layer id
	// calculations fail if we pass it the volume path and that results in very
	// cryptic errors when starting containers.
	layerID, err := wclayer.LayerID(ctx, cimPath)
	if err != nil {
		return nil, err
	}
	layers = append(layers, hcsschema.Layer{Id: layerID.String(), Path: uvmPath})
	return layers, nil
}

func containerRootfsPath(vm *uvm.UtilityVM, rootPath string) string {
	if vm.OS() == "windows" {
		return ospath.Join(vm.OS(), rootPath)
	}
	return ospath.Join(vm.OS(), rootPath, guestpath.RootfsPath)
}

func getScratchVHDPath(layerFolders []string) (string, error) {
	hostPath := filepath.Join(layerFolders[len(layerFolders)-1], "sandbox.vhdx")
	// For LCOW, we can reuse another container's scratch space (usually the sandbox container's).
	//
	// When sharing a scratch space, the `hostPath` will be a symlink to the sandbox.vhdx location to use.
	// When not sharing a scratch space, `hostPath` will be the path to the sandbox.vhdx to use.
	//
	// Evaluate the symlink here (if there is one).
	hostPath, err := filepath.EvalSymlinks(hostPath)
	if err != nil {
		return "", errors.Wrap(err, "failed to eval symlinks")
	}
	return hostPath, nil
}

// Mount the sandbox vhd to a user friendly path.
func MountSandboxVolume(ctx context.Context, hostPath, volumeName string) (err error) {
	log.G(ctx).WithFields(logrus.Fields{
		"hostpath":   hostPath,
		"volumeName": volumeName,
	}).Debug("mounting volume for container")

	if _, err := os.Stat(hostPath); os.IsNotExist(err) {
		if err := os.MkdirAll(hostPath, 0777); err != nil {
			return err
		}
	}

	defer func() {
		if err != nil {
			os.RemoveAll(hostPath)
		}
	}()

	// Make sure volumeName ends with a trailing slash as required.
	if volumeName[len(volumeName)-1] != '\\' {
		volumeName += `\` // Be nice to clients and make sure well-formed for back-compat
	}

	if err = windows.SetVolumeMountPoint(windows.StringToUTF16Ptr(hostPath), windows.StringToUTF16Ptr(volumeName)); err != nil {
		return errors.Wrapf(err, "failed to mount sandbox volume to %s on host", hostPath)
	}
	return nil
}

// Remove volume mount point. And remove folder afterwards.
func RemoveSandboxMountPoint(ctx context.Context, hostPath string) error {
	log.G(ctx).WithFields(logrus.Fields{
		"hostpath": hostPath,
	}).Debug("removing volume mount point for container")

	if err := windows.DeleteVolumeMountPoint(windows.StringToUTF16Ptr(hostPath)); err != nil {
		return errors.Wrap(err, "failed to delete sandbox volume mount point")
	}
	if err := os.Remove(hostPath); err != nil {
		return errors.Wrapf(err, "failed to remove sandbox mounted folder path %q", hostPath)
	}
	return nil
}
