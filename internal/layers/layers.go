//go:build windows
// +build windows

package layers

import (
	"context"
	"fmt"
	"os"

	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/containerd/containerd/api/types"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/mount"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/windows"
)

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

// ParseLegacyRootfsMount parses the rootfs mount format that we have traditionally
// used for both Linux and Windows containers.
// The mount format consists of:
//   - The scratch folder path in m.Source, which contains sandbox.vhdx.
//   - A mount option in the form parentLayerPaths=<JSON>, where JSON is an array of
//     string paths to read-only layer directories. The exact contents of these layer
//     directories are intepreteted differently for Linux and Windows containers.
func ParseLegacyRootfsMount(m *types.Mount) (string, []string, error) {
	// parentLayerPaths are passed in layerN, layerN-1, ..., layer 0
	//
	// The OCI spec expects:
	//   layerN, layerN-1, ..., layer0, scratch
	containerdMount := mount.Mount{Type: m.Type, Source: m.Source, Target: m.Target, Options: m.Options}
	parentLayerPaths, err := containerdMount.GetParentPaths()
	if err != nil {
		return "", nil, fmt.Errorf("failed to get mount's parent layer paths: %v", err)
	}
	return m.Source, parentLayerPaths, nil
}

// ValidateRootfsAndLayers checks to ensure we have appropriate information
// for setting up the container's root filesystem. It ensures the following:
// - One and only one of Rootfs or LayerFolders can be provided.
// - If LayerFolders are provided, there are at least two entries.
// - If Rootfs is provided, there is a single entry and it does not have a Target set.
func ValidateRootfsAndLayers(rootfs []*types.Mount, layerFolders []string) error {
	if len(rootfs) > 0 && len(layerFolders) > 0 {
		return fmt.Errorf("cannot pass both a rootfs mount and Windows.LayerFolders: %w", errdefs.ErrFailedPrecondition)
	}
	if len(rootfs) == 0 && len(layerFolders) == 0 {
		return fmt.Errorf("must pass either a rootfs mount or Windows.LayerFolders: %w", errdefs.ErrFailedPrecondition)
	}
	if len(rootfs) > 0 {
		// We have a rootfs.
		if len(rootfs) > 1 {
			return fmt.Errorf("expected a single rootfs mount: %w", errdefs.ErrFailedPrecondition)
		}
		if rootfs[0].Target != "" {
			return fmt.Errorf("rootfs mount is missing Target path: %w", errdefs.ErrFailedPrecondition)
		}
	} else {
		// We have layerFolders.
		if len(layerFolders) < 2 {
			return fmt.Errorf("must pass at least two Windows.LayerFolders: %w", errdefs.ErrFailedPrecondition)
		}
	}

	return nil
}
