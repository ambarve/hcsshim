//go:build windows
// +build windows

package layers

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/Microsoft/hcsshim/internal/guestpath"
	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/Microsoft/hcsshim/internal/ospath"
	"github.com/Microsoft/hcsshim/internal/uvm"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func addLCOWLayer(ctx context.Context, vm *uvm.UtilityVM, layerPath string) (uvmPath string, err error) {
	// don't try to add as vpmem when we want additional devices on the uvm to be fully physically backed
	if !vm.DevicesPhysicallyBacked() {
		// We first try vPMEM and if it is full or the file is too large we
		// fall back to SCSI.
		uvmPath, err = vm.AddVPMem(ctx, layerPath)
		if err == nil {
			log.G(ctx).WithFields(logrus.Fields{
				"layerPath": layerPath,
				"layerType": "vpmem",
			}).Debug("Added LCOW layer")
			return uvmPath, nil
		} else if err != uvm.ErrNoAvailableLocation && err != uvm.ErrMaxVPMemLayerSize {
			return "", fmt.Errorf("failed to add VPMEM layer: %s", err)
		}
	}

	options := []string{"ro"}
	uvmPath = fmt.Sprintf(guestpath.LCOWGlobalMountPrefixFmt, vm.UVMMountCounter())
	sm, err := vm.AddSCSI(ctx, layerPath, uvmPath, true, false, options, uvm.VMAccessTypeNoop)
	if err != nil {
		return "", fmt.Errorf("failed to add SCSI layer: %s", err)
	}
	log.G(ctx).WithFields(logrus.Fields{
		"layerPath": layerPath,
		"layerType": "scsi",
	}).Debug("Added LCOW layer")
	return sm.UVMPath, nil
}

func removeLCOWLayer(ctx context.Context, vm *uvm.UtilityVM, layerPath string) error {
	// Assume it was added to vPMEM and fall back to SCSI
	err := vm.RemoveVPMem(ctx, layerPath)
	if err == nil {
		log.G(ctx).WithFields(logrus.Fields{
			"layerPath": layerPath,
			"layerType": "vpmem",
		}).Debug("Removed LCOW layer")
		return nil
	} else if err == uvm.ErrNotAttached {
		err = vm.RemoveSCSI(ctx, layerPath)
		if err == nil {
			log.G(ctx).WithFields(logrus.Fields{
				"layerPath": layerPath,
				"layerType": "scsi",
			}).Debug("Removed LCOW layer")
			return nil
		}
		return errors.Wrap(err, "failed to remove SCSI layer")
	}
	return errors.Wrap(err, "failed to remove VPMEM layer")
}

// MountLCOWLayers is a helper for clients to hide all the complexity of layer mounting for LCOW
// Layer folder are in order: base, [rolayer1..rolayern,] scratch
// Returns the path at which the `rootfs` of the container can be accessed. Also, returns the path inside the
// UVM at which container scratch directory is located. Usually, this path is the path at which the container
// scratch VHD is mounted. However, in case of scratch sharing this is a directory under the UVM scratch.
func MountLCOWLayers(ctx context.Context, containerID string, layerFolders []string, guestRoot, volumeMountPath string, vm *uvm.UtilityVM) (_, _ string, err error) {
	if vm.OS() != "linux" {
		return "", "", errors.New("MountLCOWLayers should only be called for LCOW")
	}

	// V2 UVM
	log.G(ctx).WithField("os", vm.OS()).Debug("hcsshim::MountLCOWLayers V2 UVM")

	var (
		layersAdded       []string
		lcowUvmLayerPaths []string
	)
	defer func() {
		if err != nil {
			for _, l := range layersAdded {
				if err := removeLCOWLayer(ctx, vm, l); err != nil {
					log.G(ctx).WithError(err).Warn("failed to remove lcow layer on cleanup")
				}
			}
		}
	}()

	for _, layerPath := range layerFolders[:len(layerFolders)-1] {
		log.G(ctx).WithField("layerPath", layerPath).Debug("mounting layer")
		var (
			layerPath = filepath.Join(layerPath, "layer.vhd")
			uvmPath   string
		)
		uvmPath, err = addLCOWLayer(ctx, vm, layerPath)
		if err != nil {
			return "", "", fmt.Errorf("failed to add LCOW layer: %s", err)
		}
		layersAdded = append(layersAdded, layerPath)
		lcowUvmLayerPaths = append(lcowUvmLayerPaths, uvmPath)
	}

	containerScratchPathInUVM := ospath.Join(vm.OS(), guestRoot)
	hostPath, err := getScratchVHDPath(layerFolders)
	if err != nil {
		return "", "", fmt.Errorf("failed to get scratch VHD path in layer folders: %s", err)
	}
	log.G(ctx).WithField("hostPath", hostPath).Debug("mounting scratch VHD")

	var options []string
	scsiMount, err := vm.AddSCSI(
		ctx,
		hostPath,
		containerScratchPathInUVM,
		false,
		vm.ScratchEncryptionEnabled(),
		options,
		uvm.VMAccessTypeIndividual,
	)
	if err != nil {
		return "", "", fmt.Errorf("failed to add SCSI scratch VHD: %s", err)
	}

	// handles the case where we want to share a scratch disk for multiple containers instead
	// of mounting a new one. Pass a unique value for `ScratchPath` to avoid container upper and
	// work directories colliding in the UVM.
	if scsiMount.RefCount() > 1 {
		scratchFmt := fmt.Sprintf("container_%s", filepath.Base(containerScratchPathInUVM))
		containerScratchPathInUVM = ospath.Join("linux", scsiMount.UVMPath, scratchFmt)
	} else {
		containerScratchPathInUVM = scsiMount.UVMPath
	}

	defer func() {
		if err != nil {
			if err := vm.RemoveSCSI(ctx, hostPath); err != nil {
				log.G(ctx).WithError(err).Warn("failed to remove scratch on cleanup")
			}
		}
	}()

	rootfs := ospath.Join(vm.OS(), guestRoot, guestpath.RootfsPath)
	err = vm.CombineLayersLCOW(ctx, containerID, lcowUvmLayerPaths, containerScratchPathInUVM, rootfs)
	if err != nil {
		return "", "", err
	}
	log.G(ctx).Debug("hcsshim::MountLCOWLayers Succeeded")
	return rootfs, containerScratchPathInUVM, nil
}
