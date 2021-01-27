//go:build windows
// +build windows

package layers

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	hcsschema "github.com/Microsoft/hcsshim/internal/hcs/schema2"
	"github.com/Microsoft/hcsshim/internal/hcserror"
	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/Microsoft/hcsshim/internal/ospath"
	"github.com/Microsoft/hcsshim/internal/uvm"
	"github.com/Microsoft/hcsshim/internal/wclayer"
	cimlayer "github.com/Microsoft/hcsshim/internal/wclayer/cim"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/windows"
)

// mountArgonLayersWithRetries tries to mount argon layers with `retryCount` retries on failures.  This is
// required to handle some behavior on RS5. Loopback VHDs used to be mounted in a different manner on RS5
// (ws2019) which led to some very odd cases where things would succeed when they shouldn't have, or we'd
// simply timeout if an operation took too long. Many parallel invocations of this code path and stressing the
// machine seem to bring out the issues, but all of the possible failure paths that bring about the errors we
// have observed aren't known.
//
// On 19h1+ this *shouldn't* be needed, but the logic is to break if everything succeeded so this is harmless
// and shouldn't need a version check.
func mountArgonLayersWithRetries(ctx context.Context, scratchLayer string, parentLayers []string, retryCount int) error {
	var lErr error
	for i := 0; i < retryCount; i++ {
		lErr = func() (err error) {
			if err := wclayer.ActivateLayer(ctx, scratchLayer); err != nil {
				return err
			}

			defer func() {
				if err != nil {
					_ = wclayer.DeactivateLayer(ctx, scratchLayer)
				}
			}()

			return wclayer.PrepareLayer(ctx, scratchLayer, parentLayers)
		}()

		if lErr != nil {
			// Common errors seen from the RS5 behavior mentioned above is ERROR_NOT_READY and
			// ERROR_DEVICE_NOT_CONNECTED. The former occurs when HCS tries to grab the volume
			// path of the disk but it doesn't succeed, usually because the disk isn't actually
			// mounted. DEVICE_NOT_CONNECTED has been observed after launching multiple containers
			// in parallel on a machine under high load. This has also been observed to be a
			// trigger for ERROR_NOT_READY as well.
			if hcserr, ok := lErr.(*hcserror.HcsError); ok {
				if hcserr.Err == windows.ERROR_NOT_READY || hcserr.Err == windows.ERROR_DEVICE_NOT_CONNECTED {
					log.G(ctx).WithField("path", scratchLayer).WithError(hcserr.Err).Warning("retrying layer operations after failure")
					// Sleep for a little before a re-attempt. A probable cause for these
					// issues in the first place is events not getting reported in time so
					// might be good to give some time for things to "cool down" or get
					// back to a known state.
					time.Sleep(time.Millisecond * 100)
					continue
				}
			}
			// This was a failure case outside of the commonly known error conditions, don't retry here.
			return lErr
		}

		// No errors in layer setup, we can leave the loop
		break
	}
	// If we got unlucky and ran into one of the two errors mentioned several times in a row and left the
	// loop, we need to check the loop error here and fail also.
	if lErr != nil {
		return errors.Wrap(lErr, "layer retry loop failed")
	}
	return nil
}

// mountArgonLayers mounts the layers on the host for running argon containers. If the layers are in the cim
// format cim is mounted.
func mountArgonLayers(ctx context.Context, layerFolders []string, volumeMountPath string) (_ string, err error) {
	log.G(ctx).Debug("hcsshim::mountArgonLayers")

	if len(layerFolders) < 2 {
		return "", errors.New("need at least two layers - base and scratch")
	}
	path := layerFolders[len(layerFolders)-1]
	rest := layerFolders[:len(layerFolders)-1]
	// If layers are in the cim format mount the cim of the topmost layer.
	if strings.Contains(rest[0], "Volume") {
		rest = []string{rest[0]}
	}

	if err := mountArgonLayersWithRetries(ctx, path, rest, 5); err != nil {
		return "", err
	}

	// If any of the below fails, we want to detach the filter and unmount the disk.
	defer func() {
		if err != nil {
			_ = wclayer.UnprepareLayer(ctx, path)
			_ = wclayer.DeactivateLayer(ctx, path)
		}
	}()

	mountPath, err := wclayer.GetLayerMountPath(ctx, path)
	if err != nil {
		return "", err
	}

	// Mount the volume to a directory on the host if requested. This is the case for job containers.
	if volumeMountPath != "" {
		if err := MountSandboxVolume(ctx, volumeMountPath, mountPath); err != nil {
			return "", err
		}
	}

	return mountPath, nil
}

// mountXenonCimLayers mounts the given cim layers on the given uvm.  For cim layers there
// are two cases:
// 1. If the UVM image supports mounting the cim directly inside the uvm then share the
// directory on the host which has the cim over VSMB and then mount the cim inside the
// uvm. (This mounting will happen inside the shim)
// 2. If the UVM image is running an older windows version and doesn't support mounting
// the cim then the cim must be mounted on the host (which containerd must have already
// done). We expose that mount to the uvm over VSMB.
func mountXenonCimLayers(ctx context.Context, layerFolders []string, vm *uvm.UtilityVM) (_ string, err error) {
	if !strings.Contains(layerFolders[0], "Volume") {
		return "", fmt.Errorf("expected a path to mounted cim volume, found: %s", layerFolders[0])
	}
	if !cimlayer.IsCimLayer(layerFolders[1]) {
		return "", fmt.Errorf("mount cim layer requested for non-cim layer: %s", layerFolders[1])
	}
	// We only need to mount the topmost cim
	cimPath := cimlayer.GetCimPathFromLayer(layerFolders[1])
	options := vm.DefaultVSMBOptions(true)
	if vm.MountCimSupported() {
		// Mounting cim inside uvm needs direct map.
		options.NoDirectmap = false
		// Always add the parent directory of the cim as a vsmb mount because
		// there are region files in that directory that also should be shared in
		// the uvm.
		hostCimDir := filepath.Dir(cimPath)
		// Add the VSMB share
		if _, err := vm.AddVSMB(ctx, hostCimDir, options); err != nil {
			return "", fmt.Errorf("failed while sharing cim file inside uvm: %s", err)
		}
		defer func() {
			if err != nil {
				remErr := vm.RemoveVSMB(ctx, hostCimDir, true)
				if remErr != nil {
					log.G(ctx).WithFields(logrus.Fields{
						"host path": hostCimDir,
						"error":     remErr,
					}).Warn("failed to remove VSMB share")
				}
			}
		}()
		// get path for that share
		uvmCimDir, err := vm.GetVSMBUvmPath(ctx, hostCimDir, true)
		if err != nil {
			return "", fmt.Errorf("failed to get vsmb uvm path: %s", err)
		}
		mountCimPath, err := vm.MountInUVM(ctx, filepath.Join(uvmCimDir, filepath.Base(cimPath)))
		if err != nil {
			return "", err
		}
		return mountCimPath, nil
	} else {
		cimHostMountPath := layerFolders[0]
		if _, err := vm.AddVSMB(ctx, cimHostMountPath, options); err != nil {
			return "", fmt.Errorf("failed while sharing mounted cim inside uvm: %s", err)
		}
		// get path for that share
		cimVsmbPath, err := vm.GetVSMBUvmPath(ctx, cimHostMountPath, true)
		if err != nil {
			return "", fmt.Errorf("failed to get vsmb uvm path: %s", err)
		}
		return cimVsmbPath, nil
	}
}

// mountXenonLayersWCOW mounts the container layers inside the uvm. For legacy layers the
// layer folders are simply added as VSMB shares on the host.
func mountXenonLayersWCOW(ctx context.Context, containerID string, layerFolders []string, guestRoot string, vm *uvm.UtilityVM) (_ string, err error) {
	log.G(ctx).Debug("hcsshim::mountXenonLayersWCOW")
	var (
		layersAdded []string
	)
	defer func() {
		if err != nil {
			if err := unmountXenonWcowLayers(ctx, layerFolders, vm); err != nil {
				log.G(ctx).WithError(err).Warn("failed cleanup xenon layers")
			}
		}
	}()

	if cimlayer.IsCimLayer(layerFolders[1]) {
		_, err := mountXenonCimLayers(ctx, layerFolders, vm)
		if err != nil {
			return "", fmt.Errorf("failed to mount cim layers : %s", err)
		}
		layersAdded = append(layersAdded, layerFolders[0])
	} else {
		for _, layerPath := range layerFolders[:len(layerFolders)-1] {
			log.G(ctx).WithField("layerPath", layerPath).Debug("mounting layer")
			options := vm.DefaultVSMBOptions(true)
			options.TakeBackupPrivilege = true
			if vm.IsTemplate {
				vm.SetSaveableVSMBOptions(options, options.ReadOnly)
			}
			if _, err := vm.AddVSMB(ctx, layerPath, options); err != nil {
				return "", fmt.Errorf("failed to add VSMB layer: %s", err)
			}
			layersAdded = append(layersAdded, layerPath)

		}
	}

	containerScratchPathInUVM := ospath.Join(vm.OS(), guestRoot)
	hostPath, err := getScratchVHDPath(layerFolders)
	if err != nil {
		return "", fmt.Errorf("failed to get scratch VHD path in layer folders: %s", err)
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
		uvm.VMAccessTypeIndividual)
	if err != nil {
		return "", fmt.Errorf("failed to add SCSI scratch VHD: %s", err)
	}

	containerScratchPathInUVM = scsiMount.UVMPath

	defer func() {
		if err != nil {
			if err := vm.RemoveSCSI(ctx, hostPath); err != nil {
				log.G(ctx).WithError(err).Warn("failed to remove scratch on cleanup")
			}
		}
	}()

	// Load the filter at the C:\s<ID> location calculated above. We pass into this
	// request each of the read-only layer folders.
	var layers []hcsschema.Layer
	if cimlayer.IsCimLayer(layerFolders[1]) {
		layers, err = GetCimHCSLayer(ctx, vm, cimlayer.GetCimPathFromLayer(layerFolders[1]), layerFolders[0])
		if err != nil {
			return "", fmt.Errorf("failed to get hcs layer: %s", err)
		}
	} else {
		layers, err = GetHCSLayers(ctx, vm, layersAdded)
		if err != nil {
			return "", err
		}
	}
	err = vm.CombineLayersWCOW(ctx, layers, containerScratchPathInUVM)
	rootfs := containerScratchPathInUVM

	if err != nil {
		return "", fmt.Errorf("failed to combine layers: %s", err)
	}
	log.G(ctx).Debug("hcsshim::mountContainerLayers Succeeded")
	return rootfs, nil

}

// MountWCOWLayers is a helper for clients to hide all the complexity of layer mounting for WCOW.
// Layer folder are in order: base, [rolayer1..rolayern,] scratch
//
// v1/v2: Argon WCOW: Returns the mount path on the host as a volume GUID.
// v1:    Xenon WCOW: Done internally in HCS, so no point calling doing anything here.
// v2:    Xenon WCOW: Returns a CombinedLayersV2 structure where ContainerRootPath is a folder
//                    inside the utility VM which is a GUID mapping of the scratch folder. Each
//                    of the layers are the VSMB locations where the read-only layers are mounted.
// Job container:     Returns the mount path on the host as a volume guid, with the volume mounted on
// 					  the host at `volumeMountPath`.
func MountWCOWLayers(ctx context.Context, containerID string, layerFolders []string, guestRoot, volumeMountPath string, vm *uvm.UtilityVM) (_ string, err error) {
	if vm == nil {
		return mountArgonLayers(ctx, layerFolders, volumeMountPath)
	} else {
		return mountXenonLayersWCOW(ctx, containerID, layerFolders, guestRoot, vm)
	}
}

// unmountXenonCimLayers unmounts the given cim layers from the given uvm.  For cim layers
// there are two cases:
// 1. If the UVM image supports mounting the cim directly inside the uvm then we must have
// exposed the cim folder over VSMB and mouted the cim inside the uvm. So unmouunt the cim
// from uvm and remove that VSMB share
// 2. If the UVM image is running an older windows version and doesn't support mounting
// the cim, then we must have exposed the mounted cim on the host to the uvm over VSMB. So
// remove the VSMB mount. (containerd will take care of unmounting the cim)
func unmountXenonCimLayers(ctx context.Context, layerFolders []string, vm *uvm.UtilityVM) (err error) {
	if !strings.Contains(layerFolders[0], "Volume") {
		return fmt.Errorf("expected a path to mounted cim volume, found: %s", layerFolders[0])
	}
	if !cimlayer.IsCimLayer(layerFolders[1]) {
		return fmt.Errorf("unmount cim layer requested for non-cim layer: %s", layerFolders[1])
	}
	cimPath := cimlayer.GetCimPathFromLayer(layerFolders[1])
	if vm.MountCimSupported() {
		hostCimDir := filepath.Dir(cimPath)
		uvmCimDir, err := vm.GetVSMBUvmPath(ctx, hostCimDir, true)
		if err != nil {
			return fmt.Errorf("failed to get vsmb uvm path while mounting cim: %s", err)
		}
		if err = vm.UnmountFromUVM(ctx, filepath.Join(uvmCimDir, filepath.Base(cimPath))); err != nil {
			return errors.Wrap(err, "failed to remove cim layer from the uvm")
		}
		return vm.RemoveVSMB(ctx, hostCimDir, true)

	} else {
		if err = vm.RemoveVSMB(ctx, layerFolders[0], true); err != nil {
			log.G(ctx).Warnf("failed to remove VSMB share: %s", err)
		}
	}
	return nil
}

// unmountXenonWcowLayers unmounts the container layers inside the uvm. For legacy layers
// the layer folders are just vsmb shares and so we just need to remove that vsmb
// share.
func unmountXenonWcowLayers(ctx context.Context, layerFolders []string, vm *uvm.UtilityVM) error {
	if cimlayer.IsCimLayer(layerFolders[1]) {
		if e := unmountXenonCimLayers(ctx, layerFolders, vm); e != nil {
			return errors.Wrap(e, "failed to remove cim layers")
		}
	} else {
		for _, layerPath := range layerFolders[:len(layerFolders)-1] {
			if e := vm.RemoveVSMB(ctx, layerPath, true); e != nil {
				log.G(ctx).WithError(e).Warn("remove VSMB failed")
				return errors.Wrap(e, "failed to remove layer from the uvm")
			}
		}
	}
	return nil
}
