//go:build windows
// +build windows

package layers

import (
	"context"
	"fmt"
	"time"

	"github.com/containerd/containerd/api/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/windows"

	"github.com/Microsoft/hcsshim/internal/hcs/schema1"
	hcsschema "github.com/Microsoft/hcsshim/internal/hcs/schema2"
	"github.com/Microsoft/hcsshim/internal/hcserror"
	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/Microsoft/hcsshim/internal/resources"
	"github.com/Microsoft/hcsshim/internal/uvm"
	"github.com/Microsoft/hcsshim/internal/uvm/scsi"
	"github.com/Microsoft/hcsshim/internal/wclayer"
)

// TODO(ambarve): Maybe similar to WCOWUVMLayerManager we should add a Configure method here that will take in
// the container document and fill it with proper layer values, then we can get rid of AsHCSV*SchemaLayers
// methods. Maybe all container resources should be handled like that

// A manager for handling container layers
type WCOWLayerManager interface {
	resources.ResourceCloser
	// mounts the layers and returns the path at which mounted layers can be accessed
	Mount(ctx context.Context) (string, error)
	// returns the mounted layers in the hcs v1 schema format for use in container doc
	AsHCSV1SchemaLayers(ctx context.Context) ([]schema1.Layer, error)
	// returns the mounted layers in the hcs v2 schema format for use in container doc
	AsHCSV2SchemaLayers(ctx context.Context) ([]hcsschema.Layer, error)
}

type wcowLayerManagerCommon struct {
	containerID  string
	scratchLayer string
	// read-only image layers
	roLayers []string
}

// manager for handling for legacy layers for process isolated containers
type legacyHostLayerManager struct {
	wcowLayerManagerCommon
	volumeMountPath string
}

var _ WCOWLayerManager = &legacyHostLayerManager{}

// Only one of `layerFolders` or `rootfs` must be provided.
func newLegacyHostLayerManager(containerID string, roLayers []string, scratchLayer string, volumeMountPath string) (*legacyHostLayerManager, error) {
	return &legacyHostLayerManager{
		wcowLayerManagerCommon: wcowLayerManagerCommon{
			containerID:  containerID,
			scratchLayer: scratchLayer,
			roLayers:     roLayers,
		},
		volumeMountPath: volumeMountPath,
	}, nil
}

func (l *legacyHostLayerManager) Mount(ctx context.Context) (_ string, err error) {
	// Simple retry loop to handle some behavior on RS5. Loopback VHDs used to be mounted in a different manner on RS5 (ws2019) which led to some
	// very odd cases where things would succeed when they shouldn't have, or we'd simply timeout if an operation took too long. Many
	// parallel invocations of this code path and stressing the machine seem to bring out the issues, but all of the possible failure paths
	// that bring about the errors we have observed aren't known.
	//
	// On 19h1+ this *shouldn't* be needed, but the logic is to break if everything succeeded so this is harmless and shouldn't need a version check.
	var lErr error
	for i := 0; i < 5; i++ {
		lErr = func() (err error) {
			if err := wclayer.ActivateLayer(ctx, l.scratchLayer); err != nil {
				return err
			}

			defer func() {
				if err != nil {
					_ = wclayer.DeactivateLayer(ctx, l.scratchLayer)
				}
			}()

			return wclayer.PrepareLayer(ctx, l.scratchLayer, l.roLayers)
		}()

		if lErr != nil {
			// Common errors seen from the RS5 behavior mentioned above is ERROR_NOT_READY and ERROR_DEVICE_NOT_CONNECTED. The former occurs when HCS
			// tries to grab the volume path of the disk but it doesn't succeed, usually because the disk isn't actually mounted. DEVICE_NOT_CONNECTED
			// has been observed after launching multiple containers in parallel on a machine under high load. This has also been observed to be a trigger
			// for ERROR_NOT_READY as well.
			if hcserr, ok := lErr.(*hcserror.HcsError); ok {
				if hcserr.Err == windows.ERROR_NOT_READY || hcserr.Err == windows.ERROR_DEVICE_NOT_CONNECTED {
					log.G(ctx).WithField("path", l.scratchLayer).WithError(hcserr.Err).Warning("retrying layer operations after failure")

					// Sleep for a little before a re-attempt. A probable cause for these issues in the first place is events not getting
					// reported in time so might be good to give some time for things to "cool down" or get back to a known state.
					time.Sleep(time.Millisecond * 100)
					continue
				}
			}
			// This was a failure case outside of the commonly known error conditions, don't retry here.
			return "", lErr
		}

		// No errors in layer setup, we can leave the loop
		break
	}
	// If we got unlucky and ran into one of the two errors mentioned five times in a row and left the loop, we need to check
	// the loop error here and fail also.
	if lErr != nil {
		return "", errors.Wrap(lErr, "layer retry loop failed")
	}

	// If any of the below fails, we want to detach the filter and unmount the disk.
	defer func() {
		if err != nil {
			_ = wclayer.UnprepareLayer(ctx, l.scratchLayer)
			_ = wclayer.DeactivateLayer(ctx, l.scratchLayer)
		}
	}()

	mountPath, err := wclayer.GetLayerMountPath(ctx, l.scratchLayer)
	if err != nil {
		return "", err
	}

	// Mount the volume to a directory on the host if requested. This is the case for job containers.
	if l.volumeMountPath != "" {
		if err := MountSandboxVolume(ctx, l.volumeMountPath, mountPath); err != nil {
			return "", err
		}
	}
	return mountPath, nil
}

func (l *legacyHostLayerManager) Release(ctx context.Context) error {
	if l.volumeMountPath != "" {
		if err := RemoveSandboxMountPoint(ctx, l.volumeMountPath); err != nil {
			return err
		}
	}
	if err := wclayer.UnprepareLayer(ctx, l.scratchLayer); err != nil {
		return err
	}
	return wclayer.DeactivateLayer(ctx, l.scratchLayer)
}

func (l *legacyHostLayerManager) AsHCSV1SchemaLayers(ctx context.Context) ([]schema1.Layer, error) {
	var v1Layers []schema1.Layer
	for _, layerPath := range l.roLayers {
		layerID, err := wclayer.LayerID(ctx, layerPath)
		if err != nil {
			return nil, err
		}
		v1Layers = append(v1Layers, schema1.Layer{ID: layerID.String(), Path: layerPath})
	}
	return v1Layers, nil
}

func (l *legacyHostLayerManager) AsHCSV2SchemaLayers(ctx context.Context) ([]hcsschema.Layer, error) {
	var v2Layers []hcsschema.Layer
	for _, layerPath := range l.roLayers {
		layerID, err := wclayer.LayerID(ctx, layerPath)
		if err != nil {
			return nil, err
		}
		v2Layers = append(v2Layers, hcsschema.Layer{Id: layerID.String(), Path: layerPath})
	}
	return v2Layers, nil
}

// manager for handling for legacy layers for hyperv isolated containers
type legacyIsolatedLayerManager struct {
	wcowLayerManagerCommon
	containerScratchPathInUVM string
	vm                        *uvm.UtilityVM
	scratchMount              *scsi.Mount
	layerClosers              []resources.ResourceCloser
}

func newLegacyIsolatedLayerManager(containerID string, roLayers []string, scratchLayer string, vm *uvm.UtilityVM) (*legacyIsolatedLayerManager, error) {
	return &legacyIsolatedLayerManager{
		wcowLayerManagerCommon: wcowLayerManagerCommon{
			containerID:  containerID,
			scratchLayer: scratchLayer,
			roLayers:     roLayers,
		},
		vm: vm,
	}, nil
}

var _ WCOWLayerManager = &legacyIsolatedLayerManager{}

func (l *legacyIsolatedLayerManager) Mount(ctx context.Context) (_ string, err error) {
	log.G(ctx).WithField("os", l.vm.OS()).Debug("hcsshim::MountWCOWLayers V2 UVM")

	defer func() {
		if err != nil {
			if rErr := l.Release(ctx); rErr != nil {
				log.G(ctx).WithError(err).Warn("failed to cleanup isolated legacy layers")
			}
		}
	}()

	for _, layerPath := range l.roLayers {
		log.G(ctx).WithField("layerPath", layerPath).Debug("mounting layer")
		options := l.vm.DefaultVSMBOptions(true)
		options.TakeBackupPrivilege = true
		mount, err := l.vm.AddVSMB(ctx, layerPath, options)
		if err != nil {
			return "", fmt.Errorf("failed to add VSMB layer: %s", err)
		}
		l.layerClosers = append(l.layerClosers, mount)
	}

	log.G(ctx).WithField("hostPath", l.scratchLayer).Debug("mounting scratch VHD")

	l.scratchMount, err = l.vm.SCSIManager.AddVirtualDisk(ctx, l.scratchLayer, false, l.vm.ID(), &scsi.MountConfig{})
	if err != nil {
		return "", fmt.Errorf("failed to add SCSI scratch VHD: %s", err)
	}

	// Load the filter at the C:\s<ID> location calculated above. We pass into this
	// request each of the read-only layer folders.
	hcsLayers, err := l.AsHCSV2SchemaLayers(ctx)
	if err != nil {
		return "", err
	}
	err = l.vm.CombineLayersWCOW(ctx, hcsLayers, l.scratchMount.GuestPath())
	if err != nil {
		return "", err
	}
	log.G(ctx).Debug("hcsshim::MountWCOWLayers Succeeded")
	return l.scratchMount.GuestPath(), nil
}

func (l *legacyIsolatedLayerManager) Release(ctx context.Context) (retErr error) {
	if l.scratchMount != nil {
		if err := l.vm.RemoveCombinedLayersWCOW(ctx, l.containerScratchPathInUVM); err != nil {
			log.G(ctx).WithError(err).Error("failed RemoveCombinedLayersWCOW")
			if retErr == nil { //nolint:govet // nilness: consistency with below
				retErr = fmt.Errorf("first error: %w", err)
			}
		}

		if err := l.scratchMount.Release(ctx); err != nil {
			log.G(ctx).WithError(err).Error("failed WCOW scratch mount release")
			if retErr == nil {
				retErr = fmt.Errorf("first error: %w", err)
			}
		}
	}

	for i, closer := range l.layerClosers {
		if err := closer.Release(ctx); err != nil {
			log.G(ctx).WithFields(logrus.Fields{
				logrus.ErrorKey: err,
				"layerIndex":    i,
			}).Error("failed releasing WCOW layer")
			if retErr == nil {
				retErr = fmt.Errorf("first error: %w", err)
			}
		}
	}
	return
}

func (l *legacyIsolatedLayerManager) AsHCSV1SchemaLayers(ctx context.Context) ([]schema1.Layer, error) {
	return nil, fmt.Errorf("not supported")
}

func (l *legacyIsolatedLayerManager) AsHCSV2SchemaLayers(ctx context.Context) ([]hcsschema.Layer, error) {
	var v2Layers []hcsschema.Layer
	for _, path := range l.roLayers {
		uvmPath, err := l.vm.GetVSMBUvmPath(ctx, path, true)
		if err != nil {
			return nil, err
		}
		layerID, err := wclayer.LayerID(ctx, path)
		if err != nil {
			return nil, err
		}
		v2Layers = append(v2Layers, hcsschema.Layer{Id: layerID.String(), Path: uvmPath})
	}
	return v2Layers, nil
}

// only one of `layerFolders` or `rootfs` MUST be provided. We accept both to maintain compatibility with old code.
func NewWCOWLayerManager(containerID string, rootfs []*types.Mount, layerFolders []string, vm *uvm.UtilityVM, volumeMountPath string) (WCOWLayerManager, error) {
	err := ValidateRootfsAndLayers(rootfs, layerFolders)
	if err != nil {
		return nil, err
	}

	var roLayers []string
	var scratchLayer string
	if len(layerFolders) > 0 {
		scratchLayer, roLayers = layerFolders[len(layerFolders)-1], layerFolders[:len(layerFolders)-1]
	} else {
		scratchLayer, roLayers, err = ParseLegacyRootfsMount(rootfs[0])
		if err != nil {
			return nil, err
		}
	}

	if vm == nil {
		return newLegacyHostLayerManager(containerID, roLayers, scratchLayer, volumeMountPath)
	}
	return newLegacyIsolatedLayerManager(containerID, roLayers, scratchLayer, vm)
}
