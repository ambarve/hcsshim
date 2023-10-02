//go:build windows

package uvm

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	hcsschema "github.com/Microsoft/hcsshim/internal/hcs/schema2"
	"github.com/Microsoft/hcsshim/internal/layers"
	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/Microsoft/hcsshim/internal/protocol/guestrequest"
	"github.com/Microsoft/hcsshim/internal/uvm/scsi"
	"github.com/Microsoft/hcsshim/internal/wclayer"
	"github.com/Microsoft/hcsshim/internal/wcow"
	"github.com/containerd/containerd/api/types"
	"github.com/sirupsen/logrus"
)

// A manager for handling the layers of a windows UVM
type WCOWUVMLayerManager interface {
	// Configure takes in a UVM config of a UVM that is about to boot
	// and sets it up properly by mounting the UVM layers. (if required)
	// The UtilityVM instance is modified to account for newly added SCSI disks/VSMB shares etc.
	Configure(context.Context, *UtilityVM, *hcsschema.ComputeSystem) error
}

type legacyUVMLayerManager struct {
	roLayers     []string
	scratchLayer string
}

// locateUVMFolder searches a set of layer folders to determine the "uppermost"
// layer which has a utility VM image. The order of the layers is (for historical) reasons
// Read-only-layers followed by an optional read-write layer. The RO layers are in reverse
// order so that the upper-most RO layer is at the start, and the base OS layer is the
// end.
func locateUVMFolder(ctx context.Context, layerFolders []string) (string, error) {
	var uvmFolder string
	index := 0
	for _, layerFolder := range layerFolders {
		_, err := os.Stat(filepath.Join(layerFolder, `UtilityVM`))
		if err == nil {
			uvmFolder = layerFolder
			break
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		index++
	}
	if uvmFolder == "" {
		return "", fmt.Errorf("utility VM folder could not be found in layers")
	}

	log.G(ctx).WithFields(logrus.Fields{
		"index":  index + 1,
		"count":  len(layerFolders),
		"folder": uvmFolder,
	}).Debug("hcsshim::LocateUVMFolder: found")
	return uvmFolder, nil
}

// Configure implements WCOWUVMLayerManager
func (l *legacyUVMLayerManager) Configure(ctx context.Context, uvm *UtilityVM, doc *hcsschema.ComputeSystem) error {
	if uvm.id == "" {
		// UVM struct must be initialized to have a valid ID before calling this method
		panic("UVM ID must be initialized")
	}

	// In non-CRI cases the UVM's scratch VHD will be created in the same directory as that of the container scratch, since both are named "sandbox.vhdx" we create a directory named "vm" and store UVM scratch there.
	// TODO: BUGBUG Remove this. @jhowardmsft
	//       It should be the responsibility of the caller to do the creation and population.
	//       - Update runhcs too (vm.go).
	//       - Remove comment in function header
	//       - Update tests that rely on this current behavior.
	vmPath := filepath.Join(l.scratchLayer, "vm")
	err := os.MkdirAll(vmPath, 0)
	if err != nil {
		return err
	}

	uvmFolder, err := locateUVMFolder(ctx, l.roLayers)
	if err != nil {
		return fmt.Errorf("failed to locate utility VM folder from layer folders: %s", err)
	}

	// Create sandbox.vhdx in the scratch folder based on the template, granting the correct permissions to it
	scratchPath := filepath.Join(l.scratchLayer, "sandbox.vhdx")
	if _, err := os.Stat(scratchPath); os.IsNotExist(err) {
		if err := wcow.CreateUVMScratch(ctx, uvmFolder, l.scratchLayer, uvm.id); err != nil {
			return fmt.Errorf("failed to create scratch: %s", err)
		}
	} else {
		// Sandbox.vhdx exists, just need to grant vm access to it.
		if err := wclayer.GrantVmAccess(ctx, uvm.id, scratchPath); err != nil {
			return fmt.Errorf("failed to grant vm access to scratch: %w", err)
		}
	}

	doc.VirtualMachine.Devices.Scsi = map[string]hcsschema.Scsi{}
	for i := 0; i < int(uvm.scsiControllerCount); i++ {
		doc.VirtualMachine.Devices.Scsi[guestrequest.ScsiControllerGuids[i]] = hcsschema.Scsi{
			Attachments: make(map[string]hcsschema.Attachment),
		}
	}

	doc.VirtualMachine.Devices.Scsi[guestrequest.ScsiControllerGuids[0]].Attachments["0"] = hcsschema.Attachment{

		Path:  scratchPath,
		Type_: "VirtualDisk",
	}

	uvm.reservedSCSISlots = append(uvm.reservedSCSISlots, scsi.Slot{Controller: 0, LUN: 0})

	// UVM rootfs share is readonly.
	vsmbOpts := uvm.DefaultVSMBOptions(true)
	vsmbOpts.TakeBackupPrivilege = true
	virtualSMB := &hcsschema.VirtualSmb{
		DirectFileMappingInMB: 1024, // Sensible default, but could be a tuning parameter somewhere
		Shares: []hcsschema.VirtualSmbShare{
			{
				Name:    "os",
				Path:    filepath.Join(uvmFolder, `UtilityVM\Files`),
				Options: vsmbOpts,
			},
		},
	}

	if doc.VirtualMachine.Devices == nil {
		doc.VirtualMachine.Devices = &hcsschema.Devices{}
	}
	doc.VirtualMachine.Devices.VirtualSmb = virtualSMB

	return nil
}

// Only one of the `layerFolders` or `rootfs` MUST be provided. If `layerFolders` is
// provided a legacy layer manager will be returned. If `rootfs` is provided a layer manager
// based on the type of mount will be returned
func NewWCOWUVMLayerManager(layerFolders []string, rootfs []*types.Mount) (WCOWUVMLayerManager, error) {
	err := layers.ValidateRootfsAndLayers(rootfs, layerFolders)
	if err != nil {
		return nil, err
	}

	var roLayers []string
	var scratchLayer string
	if len(layerFolders) > 0 {
		scratchLayer, roLayers = layerFolders[len(layerFolders)-1], layerFolders[:len(layerFolders)-1]
	} else {
		scratchLayer, roLayers, err = layers.ParseLegacyRootfsMount(rootfs[0])
		if err != nil {
			return nil, err
		}
	}

	return &legacyUVMLayerManager{
		roLayers:     roLayers,
		scratchLayer: scratchLayer,
	}, nil
}
