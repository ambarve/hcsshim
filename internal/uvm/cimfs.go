package uvm

import (
	"context"
	"fmt"

	"github.com/Microsoft/go-winio/pkg/guid"
	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/Microsoft/hcsshim/internal/protocol/guestrequest"
	"github.com/Microsoft/hcsshim/internal/protocol/guestresource"
	"github.com/Microsoft/hcsshim/internal/uvm/scsi"
	"github.com/Microsoft/hcsshim/pkg/cimfs"
	"github.com/sirupsen/logrus"
)

type UVMMountedBlockCIMs struct {
	scsiMounts []*scsi.Mount
	// Volume Path inside the UVM at which the CIMs are mounted
	VolumePath string
}

func (umb *UVMMountedBlockCIMs) Release(ctx context.Context) error {
	for i := len(umb.scsiMounts) - 1; i >= 0; i-- {
		if err := umb.scsiMounts[i].Release(ctx); err != nil {
			return err
		}
	}
	return nil
}

// mergedCIM can be nil,
// sourceCIMs MUST be in the top to bottom order
func (uvm *UtilityVM) MountBlockCIMs(ctx context.Context, mergedCIM *cimfs.BlockCIM, sourceCIMs []*cimfs.BlockCIM) (_ *UVMMountedBlockCIMs, err error) {
	volumeGUID, err := guid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("generated cim mount GUID: %w", err)
	}

	layersToAttach := sourceCIMs
	if mergedCIM != nil {
		layersToAttach = append([]*cimfs.BlockCIM{mergedCIM}, sourceCIMs...)
	}

	settings := &guestresource.WCOWBlockCIMMounts{
		BlockCIMs:  []guestresource.BlockCIMDevice{},
		VolumeGuid: volumeGUID,
		MountFlags: cimfs.CimMountBlockDeviceCim,
	}

	umb := &UVMMountedBlockCIMs{
		VolumePath: fmt.Sprintf(cimfs.VolumePathFormat, volumeGUID.String()),
		scsiMounts: []*scsi.Mount{},
	}

	for _, bcim := range layersToAttach {
		sm, err := uvm.SCSIManager.AddVirtualDisk(ctx, bcim.BlockPath, true, uvm.ID(), "", nil)
		if err != nil {
			return nil, fmt.Errorf("failed to attach block CIM %s: %w", bcim.BlockPath, err)
		}
		log.G(ctx).WithFields(logrus.Fields{
			"block path":      bcim.BlockPath,
			"cim name":        bcim.CimName,
			"scsi controller": sm.Controller(),
			"scsi LUN":        sm.LUN(),
		}).Debugf("attached block CIM VHD")
		settings.BlockCIMs = append(settings.BlockCIMs, guestresource.BlockCIMDevice{
			CimName: bcim.CimName,
			Lun:     int32(sm.LUN()),
		})
		umb.scsiMounts = append(umb.scsiMounts, sm)
		defer func() {
			if err != nil {
				sm.Release(ctx)
			}
		}()
	}

	guestReq := guestrequest.ModificationRequest{
		ResourceType: guestresource.ResourceTypeWCOWBlockCims,
		RequestType:  guestrequest.RequestTypeAdd,
		Settings:     settings,
	}
	if err := uvm.GuestRequest(ctx, guestReq); err != nil {
		return nil, fmt.Errorf("failed to mount the cim: %w", err)
	}
	return umb, nil
}
