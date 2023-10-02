//go:build windows
// +build windows

package layers

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/Microsoft/hcsshim/internal/guestpath"
	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/Microsoft/hcsshim/internal/ospath"
	"github.com/Microsoft/hcsshim/internal/resources"
	uvmpkg "github.com/Microsoft/hcsshim/internal/uvm"
	"github.com/Microsoft/hcsshim/internal/uvm/scsi"
	"github.com/containerd/containerd/api/types"
)

type lcowLayer struct {
	VHDPath   string
	Partition uint64
}

// LCOWLayerManager isn't an interface like WCOWLayerManager because currently there aren't multiple layer implementations for LCOW
type LCOWLayerManager struct {
	containerID string
	// Should be in order from top-most layer to bottom-most layer.
	roLayers []*lcowLayer
	// path on the host where VHD is stored
	scratchVHDPath string
	vm             *uvmpkg.UtilityVM
	// path inside the UVM which represents container's scratch directory. This could either be a path at
	// which the container scratch VHD is mounted or it could be a simple directory if scratch VHD is
	// being shared.
	// different from the scratchMount.guestPath)
	containerScratchInUVM string
	// path inside the UVM where the overlayfs mounts the container's rootfs
	rootfs string
	// the directory which holds all of the container related data (for e.g. rootfs directory is created inside this containerRoot)
	containerRoot string
	scratchMount  *scsi.Mount
	layerClosers  []resources.ResourceCloser
}

func toLCOWLayers(parentLayers []string) []*lcowLayer {
	// Each read-only layer should have a layer.vhd, and the scratch layer should have a sandbox.vhdx.
	roLayers := make([]*lcowLayer, 0, len(parentLayers))
	for _, parentLayer := range parentLayers {
		roLayers = append(
			roLayers,
			&lcowLayer{
				VHDPath: filepath.Join(parentLayer, "layer.vhd"),
			},
		)
	}
	return roLayers
}

// only one of `layerFolders` or `rootfs` MUST be provided. We accept both to maintain compatibility with old code.
func NewLCOWLayerManager(containerID string, rootfs []*types.Mount, layerFolders []string, uvm *uvmpkg.UtilityVM) (*LCOWLayerManager, error) {
	if uvm == nil {
		return nil, errors.New("MountLCOWLayers cannot be called for process-isolated containers")
	}

	if uvm.OS() != "linux" {
		return nil, errors.New("MountLCOWLayers should only be called for LCOW")
	}

	lm := &LCOWLayerManager{
		containerID:   containerID,
		vm:            uvm,
		containerRoot: fmt.Sprintf(guestpath.LCOWRootPrefixInUVM+"/%s", containerID),
	}
	if len(layerFolders) > 0 {
		lm.roLayers = toLCOWLayers(layerFolders[:len(layerFolders)-1])
		lm.scratchVHDPath = filepath.Join(layerFolders[len(layerFolders)-1], "sandbox.vhdx")
	} else {
		switch m.Type {
		case "lcow-layer":
			scratchLayer, parentLayers, err := ParseLegacyRootfsMount(m)
			if err != nil {
				return nil, err
			}
			lm.roLayers = toLCOWLayers(parentLayers)
			lm.scratchVHDPath = filepath.Join(scratchLayer, "sandbox.vhdx")
		case "lcow-partitioned-layer":
			var (
				scratchPath string
				layerData   []struct {
					Path      string
					Partition uint64
				}
			)
			for _, opt := range m.Options {
				if optPrefix := "scratch="; strings.HasPrefix(opt, optPrefix) {
					scratchPath = strings.TrimPrefix(opt, optPrefix)
				} else if optPrefix := "parent-partitioned-layers="; strings.HasPrefix(opt, optPrefix) {
					layerJSON := strings.TrimPrefix(opt, optPrefix)
					if err := json.Unmarshal([]byte(layerJSON), &layerData); err != nil {
						return nil, err
					}
				} else {
					return nil, fmt.Errorf("unrecognized %s mount option: %s", m.Type, opt)
				}
			}
			roLayers := make([]*lcowLayer, 0, len(layerData))
			for _, layer := range layerData {
				roLayers = append(
					roLayers,
					&lcowLayer{
						VHDPath:   layer.Path,
						Partition: layer.Partition,
					},
				)
			}
			lm.roLayers = roLayers
			lm.scratchVHDPath = scratchPath
		default:
			return nil, fmt.Errorf("unrecognized rootfs mount type: %s", m.Type)
		}
	}
	return lm, nil
}

func (lc *LCOWLayerManager) ContainerRoot() string {
	return lc.containerRoot
}

func (lc *LCOWLayerManager) Release(ctx context.Context) (retErr error) {
	if lc.rootfs != "" {
		if err := lc.vm.RemoveCombinedLayersLCOW(ctx, lc.rootfs); err != nil {
			log.G(ctx).WithError(err).Error("failed RemoveCombinedLayersLCOW")
			if retErr == nil { //nolint:govet // nilness: consistency with below
				retErr = fmt.Errorf("first error: %w", err)
			}
		}
	}

	if lc.scratchMount != nil {
		if err := lc.scratchMount.Release(ctx); err != nil {
			log.G(ctx).WithError(err).Error("failed LCOW scratch mount release")
			if retErr == nil {
				retErr = fmt.Errorf("first error: %w", err)
			}
		}
	}

	for i, closer := range lc.layerClosers {
		if err := closer.Release(ctx); err != nil {
			log.G(ctx).WithFields(logrus.Fields{
				logrus.ErrorKey: err,
				"layerIndex":    i,
			}).Error("failed releasing LCOW layer")
			if retErr == nil {
				retErr = fmt.Errorf("first error: %w", err)
			}
		}
	}
	return
}

// MountLCOWLayers is a helper for clients to hide all the complexity of layer mounting for LCOW
// Layer folder are in order: base, [rolayer1..rolayern,] scratch
// Returns the path at which the `rootfs` of the container can be accessed. Also, returns the path inside the
// UVM at which container scratch directory is located. Usually, this path is the path at which the container
// scratch VHD is mounted. However, in case of scratch sharing this is a directory under the UVM scratch.
func (l *LCOWLayerManager) Mount(ctx context.Context) (_ string, err error) {
	// V2 UVM
	log.G(ctx).WithField("os", l.vm.OS()).Debug("hcsshim::MountLCOWLayers V2 UVM")

	var (
		lcowUvmLayerPaths []string
	)
	defer func() {
		if err != nil {
			if rErr := l.Release(ctx); rErr != nil {
				log.G(ctx).WithError(err).Warn("failed to cleanup lcow layers")
			}
		}
	}()

	for _, layer := range l.roLayers {
		log.G(ctx).WithField("layerPath", layer.VHDPath).Debug("mounting layer")
		uvmPath, closer, err := addLCOWLayer(ctx, l.vm, layer)
		if err != nil {
			return "", fmt.Errorf("failed to add LCOW layer: %s", err)
		}
		l.layerClosers = append(l.layerClosers, closer)
		lcowUvmLayerPaths = append(lcowUvmLayerPaths, uvmPath)
	}

	hostPath := l.scratchVHDPath
	hostPath, err = filepath.EvalSymlinks(hostPath)
	if err != nil {
		return "", fmt.Errorf("failed to eval symlinks on scratch path: %w", err)
	}
	log.G(ctx).WithField("hostPath", hostPath).Debug("mounting scratch VHD")

	mConfig := &scsi.MountConfig{
		Encrypted: l.vm.ScratchEncryptionEnabled(),
		// For scratch disks, we support formatting the disk if it is not already
		// formatted.
		EnsureFilesystem: true,
		Filesystem:       "ext4",
	}
	if l.vm.ScratchEncryptionEnabled() {
		// Encrypted scratch devices are formatted with xfs
		mConfig.Filesystem = "xfs"
	}
	l.scratchMount, err = l.vm.SCSIManager.AddVirtualDisk(
		ctx,
		hostPath,
		false,
		l.vm.ID(),
		mConfig,
	)
	if err != nil {
		return "", fmt.Errorf("failed to add SCSI scratch VHD: %s", err)
	}

	// handles the case where we want to share a scratch disk for multiple containers instead
	// of mounting a new one. Pass a unique value for `ScratchPath` to avoid container upper and
	// work directories colliding in the UVM.
	l.containerScratchInUVM = ospath.Join("linux", l.scratchMount.GuestPath(), "scratch", l.containerID)

	l.rootfs = ospath.Join(l.vm.OS(), l.containerRoot, guestpath.RootfsPath)
	err = l.vm.CombineLayersLCOW(ctx, l.containerID, lcowUvmLayerPaths, l.containerScratchInUVM, l.rootfs)
	if err != nil {
		return "", err
	}
	log.G(ctx).Debug("hcsshim::MountLCOWLayers Succeeded")
	return l.rootfs, nil
}

func addLCOWLayer(ctx context.Context, vm *uvmpkg.UtilityVM, layer *lcowLayer) (uvmPath string, _ resources.ResourceCloser, err error) {
	// Don't add as VPMEM when we want additional devices on the UVM to be fully physically backed.
	// Also don't use VPMEM when we need to mount a specific partition of the disk, as this is only
	// supported for SCSI.
	if !vm.DevicesPhysicallyBacked() && layer.Partition == 0 {
		// We first try vPMEM and if it is full or the file is too large we
		// fall back to SCSI.
		mount, err := vm.AddVPMem(ctx, layer.VHDPath)
		if err == nil {
			log.G(ctx).WithFields(logrus.Fields{
				"layerPath": layer.VHDPath,
				"layerType": "vpmem",
			}).Debug("Added LCOW layer")
			return mount.GuestPath, mount, nil
		} else if err != uvmpkg.ErrNoAvailableLocation && err != uvmpk.ErrMaxVPMemLayerSize {
			return "", nil, fmt.Errorf("failed to add VPMEM layer: %s", err)
		}
	}

	sm, err := vm.SCSIManager.AddVirtualDisk(
		ctx,
		layer.VHDPath,
		true,
		"",
		&scsi.MountConfig{
			Partition: layer.Partition,
			Options:   []string{"ro"},
		},
	)
	if err != nil {
		return "", nil, fmt.Errorf("failed to add SCSI layer: %s", err)
	}
	log.G(ctx).WithFields(logrus.Fields{
		"layerPath":      layer.VHDPath,
		"layerPartition": layer.Partition,
		"layerType":      "scsi",
	}).Debug("Added LCOW layer")
	return sm.GuestPath(), sm, nil
}
