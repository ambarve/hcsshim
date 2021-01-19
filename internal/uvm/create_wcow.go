package uvm

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Microsoft/go-winio"
	"github.com/Microsoft/go-winio/pkg/guid"
	"github.com/Microsoft/hcsshim/internal/cimfs"
	"github.com/Microsoft/hcsshim/internal/gcs"
	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/Microsoft/hcsshim/internal/logfields"
	"github.com/Microsoft/hcsshim/internal/mergemaps"
	"github.com/Microsoft/hcsshim/internal/oc"
	"github.com/Microsoft/hcsshim/internal/processorinfo"
	hcsschema "github.com/Microsoft/hcsshim/internal/schema2"
	"github.com/Microsoft/hcsshim/internal/schemaversion"
	"github.com/Microsoft/hcsshim/internal/uvmfolder"
	"github.com/Microsoft/hcsshim/internal/wclayer"
	cimlayer "github.com/Microsoft/hcsshim/internal/wclayer/cim"
	"github.com/Microsoft/hcsshim/internal/wcow"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
)

const cimVsmbShareName = "bootcimdir"

// OptionsWCOW are the set of options passed to CreateWCOW() to create a utility vm.
type OptionsWCOW struct {
	*Options

	LayerFolders []string // Set of folders for base layers and scratch. Ordered from top most read-only through base read-only layer, followed by scratch
}

// NewDefaultOptionsWCOW creates the default options for a bootable version of
// WCOW. The caller `MUST` set the `LayerFolders` path on the returned value.
//
// `id` the ID of the compute system. If not passed will generate a new GUID.
//
// `owner` the owner of the compute system. If not passed will use the
// executable files name.
func NewDefaultOptionsWCOW(id, owner string) *OptionsWCOW {
	return &OptionsWCOW{
		Options: newDefaultOptions(id, owner),
	}
}

// mountUvmCimLayers mounts the cim layers for use of the uvm and returns the new set of
// layers which contain the path to the mounted cim.
func mountUvmCimLayers(ctx context.Context, layerFolders []string) (_ []string, err error) {
	cimLayers := []string{}
	cimPath := cimlayer.GetCimPathFromLayer(layerFolders[0])
	cimMountPath, err := cimfs.Mount(cimPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			cimfs.UnMount(cimPath)
		}
	}()

	cimLayers = append(cimLayers, cimMountPath)
	cimLayers = append(cimLayers, layerFolders[len(layerFolders)-1])
	return cimLayers, nil
}

// addBootFromCimRegistryChanges adds several registry keys to make the uvm directly
// boot from a cim. Note that this is only supported for IRON+ uvms. Details of these keys
// are as follows:
// 1. To notify the uvm that this boot should happen directly from a cim:
// - ControlSet001\Control\HVSI /v WCIFSCIMFSContainerMode /t REG_DWORD /d 0x1
// - ControlSet001\Control\HVSI /v WCIFSContainerMode /t REG_DWORD /d 0x1
// 2. We also need to provide the path inside the uvm at which this cim can be
// accessed. In order to share the cim inside the uvm at boot time we always add a vsmb
// share by name `$cimVsmbShareName` into the uvm to share the directory which contains
// the cim of that layer. This registry key should specify a path whose first element is
// the name of that share and the second element is the name of the cim.
// - ControlSet001\Control\HVSI /v CimRelativePath /t REG_SZ /d  $CimVsmbShareName`+\\+`$nameofthelayercim`
// 3. A cim that is shared inside the uvm includes files for both the uvm and the
// containers. All the files for the uvm are kept inside the `UtilityVM\Files` directory
// so below registry key specifies the name of this directory inside the cim which
// contains all the uvm related files.
// - ControlSet001\Control\HVSI /v UvmLayerRelativePath /t REG_SZ /d UtilityVM\\Files\\ (the ending \\ is important)
func addBootFromCimRegistryChanges(layerFolders []string, reg *hcsschema.RegistryChanges) {
	cimRelativePath := cimVsmbShareName + "\\" + cimlayer.GetCimNameFromLayer(layerFolders[0])

	regChanges := []hcsschema.RegistryValue{
		{
			Key: &hcsschema.RegistryKey{
				Hive: "System",
				Name: "ControlSet001\\Control\\HVSI",
			},
			Name:       "WCIFSCIMFSContainerMode",
			Type_:      "DWord",
			DWordValue: 1,
		},
		{
			Key: &hcsschema.RegistryKey{
				Hive: "System",
				Name: "ControlSet001\\Control\\HVSI",
			},
			Name:       "WCIFSContainerMode",
			Type_:      "DWord",
			DWordValue: 1,
		},
		{
			Key: &hcsschema.RegistryKey{
				Hive: "System",
				Name: "ControlSet001\\Control\\HVSI",
			},
			Name:        "CimRelativePath",
			Type_:       "String",
			StringValue: cimRelativePath,
		},
		{
			Key: &hcsschema.RegistryKey{
				Hive: "System",
				Name: "ControlSet001\\Control\\HVSI",
			},
			Name:        "UvmLayerRelativePath",
			Type_:       "String",
			StringValue: "UtilityVM\\Files\\",
		},
	}

	reg.AddValues = append(reg.AddValues, regChanges...)
}

// CreateWCOW creates an HCS compute system representing a utility VM.
//
// WCOW Notes:
//   - The scratch is always attached to SCSI 0:0
//
func CreateWCOW(ctx context.Context, opts *OptionsWCOW) (_ *UtilityVM, err error) {
	ctx, span := trace.StartSpan(ctx, "uvm::CreateWCOW")
	defer span.End()
	defer func() { oc.SetSpanStatus(span, err) }()

	if opts.ID == "" {
		g, err := guid.NewV4()
		if err != nil {
			return nil, err
		}
		opts.ID = g.String()
	}

	span.AddAttributes(trace.StringAttribute(logfields.UVMID, opts.ID))
	log.G(ctx).WithField("options", fmt.Sprintf("%+v", opts)).Debug("uvm::CreateWCOW options")

	uvm := &UtilityVM{
		id:                      opts.ID,
		owner:                   opts.Owner,
		operatingSystem:         "windows",
		scsiControllerCount:     1,
		vsmbDirShares:           make(map[string]*VSMBShare),
		vsmbFileShares:          make(map[string]*VSMBShare),
		vpciDevices:             make(map[string]*VPCIDevice),
		physicallyBacked:        !opts.AllowOvercommit,
		devicesPhysicallyBacked: opts.FullyPhysicallyBacked,
		cpuGroupID:              opts.CPUGroupID,
		cimMounts:               make(map[string]*cimInfo),
		layerFolders:            opts.LayerFolders,
	}

	defer func() {
		if err != nil {
			uvm.Close()
		}
	}()

	if err := verifyOptions(ctx, opts); err != nil {
		return nil, errors.Wrap(err, errBadUVMOpts.Error())
	}

	uvmLayers := opts.LayerFolders
	templateVhdFolder, err := uvmfolder.LocateUVMFolder(ctx, uvmLayers)
	if err != nil {
		return nil, fmt.Errorf("failed to locate utility VM folder from layer folders: %s", err)
	}

	vsmbOpts := uvm.DefaultVSMBOptions(true)
	vsmbOpts.TakeBackupPrivilege = true
	uvmFolder := templateVhdFolder
	if cimlayer.IsCimLayer(opts.LayerFolders[0]) {
		uvmLayers, err = mountUvmCimLayers(ctx, opts.LayerFolders)
		uvmFolder, err = uvmfolder.LocateUVMFolder(ctx, uvmLayers)
		if err != nil {
			return nil, fmt.Errorf("failed to locate utility VM folder from cim layer folders: %s", err)
		}
		vsmbOpts.NoDirectmap = !uvm.MountCimSupported()
	}

	// TODO: BUGBUG Remove this. @jhowardmsft
	//       It should be the responsiblity of the caller to do the creation and population.
	//       - Update runhcs too (vm.go).
	//       - Remove comment in function header
	//       - Update tests that rely on this current behaviour.
	// Create the RW scratch in the top-most layer folder, creating the folder if it doesn't already exist.
	scratchFolder := opts.LayerFolders[len(opts.LayerFolders)-1]

	// Create the directory if it doesn't exist
	if _, err := os.Stat(scratchFolder); os.IsNotExist(err) {
		if err := os.MkdirAll(scratchFolder, 0777); err != nil {
			return nil, fmt.Errorf("failed to create utility VM scratch folder: %s", err)
		}
	}

	// Create sandbox.vhdx in the scratch folder based on the template, granting the correct permissions to it
	scratchPath := filepath.Join(scratchFolder, "sandbox.vhdx")
	if _, err := os.Stat(scratchPath); os.IsNotExist(err) {
		if err := wcow.CreateUVMScratch(ctx, templateVhdFolder, scratchFolder, uvm.id); err != nil {
			return nil, fmt.Errorf("failed to create scratch: %s", err)
		}
	} else {
		// Sandbox.vhdx exists, just need to grant vm access to it.
		if err := wclayer.GrantVmAccess(ctx, uvm.id, scratchPath); err != nil {
			return nil, errors.Wrap(err, "failed to grant vm access to scratch")
		}
	}

	processorTopology, err := processorinfo.HostProcessorInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get host processor information: %s", err)
	}

	// To maintain compatability with Docker we need to automatically downgrade
	// a user CPU count if the setting is not possible.
	uvm.processorCount = uvm.normalizeProcessorCount(ctx, opts.ProcessorCount, processorTopology)

	// Align the requested memory size.
	memorySizeInMB := uvm.normalizeMemorySize(ctx, opts.MemorySizeInMB)

	// UVM rootfs share & bootcim shares are readonly.
	virtualSMB := &hcsschema.VirtualSmb{
		DirectFileMappingInMB: 2048, // Sensible default, but could be a tuning parameter somewhere
		Shares: []hcsschema.VirtualSmbShare{
			{
				Name:    "os",
				Path:    filepath.Join(uvmFolder, `UtilityVM\Files`),
				Options: vsmbOpts,
			},
		},
	}
	uvm.registerVSMBShare(filepath.Join(uvmFolder, `UtilityVM\Files`), vsmbOpts, "os")

	// Here for a temporary workaround until the need for setting this regkey is no more. To protect
	// against any undesired behavior (such as some general networking scenarios ceasing to function)
	// with a recent change to fix SMB share access in the UVM, this registry key will be checked to
	// enable the change in question inside GNS.dll.
	var registryChanges hcsschema.RegistryChanges
	if !opts.DisableCompartmentNamespace {
		registryChanges = hcsschema.RegistryChanges{
			AddValues: []hcsschema.RegistryValue{
				{
					Key: &hcsschema.RegistryKey{
						Hive: "System",
						Name: "CurrentControlSet\\Services\\gns",
					},
					Name:       "EnableCompartmentNamespace",
					DWordValue: 1,
					Type_:      "DWord",
				},
			},
		}
	}

	if uvm.MountCimSupported() {
		// If mount cim is supported then we must include a VSMB share in uvm
		// config that contains the cim which the uvm should use to boot.
		cimVsmbShare := hcsschema.VirtualSmbShare{
			Name:    cimVsmbShareName,
			Path:    cimlayer.GetCimDirFromLayer(opts.LayerFolders[0]),
			Options: vsmbOpts,
		}
		virtualSMB.Shares = append(virtualSMB.Shares, cimVsmbShare)
		uvm.registerVSMBShare(cimlayer.GetCimDirFromLayer(opts.LayerFolders[0]), vsmbOpts, cimVsmbShareName)

		// enable boot from cim
		addBootFromCimRegistryChanges(opts.LayerFolders, &registryChanges)
	}

	doc := &hcsschema.ComputeSystem{
		Owner:                             uvm.owner,
		SchemaVersion:                     schemaversion.SchemaV21(),
		ShouldTerminateOnLastHandleClosed: true,
		VirtualMachine: &hcsschema.VirtualMachine{
			StopOnReset: true,
			Chipset: &hcsschema.Chipset{
				Uefi: &hcsschema.Uefi{
					BootThis: &hcsschema.UefiBootEntry{
						DevicePath: `\EFI\Microsoft\Boot\bootmgfw.efi`,
						DeviceType: "VmbFs",
					},
				},
			},
			RegistryChanges: &registryChanges,
			ComputeTopology: &hcsschema.Topology{
				Memory: &hcsschema.Memory2{
					SizeInMB:        memorySizeInMB,
					AllowOvercommit: opts.AllowOvercommit,
					// EnableHotHint is not compatible with physical.
					EnableHotHint:        opts.AllowOvercommit,
					EnableDeferredCommit: opts.EnableDeferredCommit,
					LowMMIOGapInMB:       opts.LowMMIOGapInMB,
					HighMMIOBaseInMB:     opts.HighMMIOBaseInMB,
					HighMMIOGapInMB:      opts.HighMMIOGapInMB,
				},
				Processor: &hcsschema.Processor2{
					Count:  uvm.processorCount,
					Limit:  opts.ProcessorLimit,
					Weight: opts.ProcessorWeight,
				},
			},
			Devices: &hcsschema.Devices{
				ComPorts: map[string]hcsschema.ComPort{
					"0": {
						NamedPipe: "\\\\.\\pipe\\debugpipe",
					},
				},
				Scsi: map[string]hcsschema.Scsi{
					"0": {
						Attachments: map[string]hcsschema.Attachment{
							"0": {
								Path:  scratchPath,
								Type_: "VirtualDisk",
							},
						},
					},
				},
				HvSocket: &hcsschema.HvSocket2{
					HvSocketConfig: &hcsschema.HvSocketSystemConfig{
						// Allow administrators and SYSTEM to bind to vsock sockets
						// so that we can create a GCS log socket.
						DefaultBindSecurityDescriptor: "D:P(A;;FA;;;SY)(A;;FA;;;BA)",
					},
				},
				VirtualSmb: virtualSMB,
			},
		},
	}

	if !opts.ExternalGuestConnection {
		doc.VirtualMachine.GuestConnection = &hcsschema.GuestConnection{}
	}

	// Handle StorageQoS if set
	if opts.StorageQoSBandwidthMaximum > 0 || opts.StorageQoSIopsMaximum > 0 {
		doc.VirtualMachine.StorageQoS = &hcsschema.StorageQoS{
			IopsMaximum:      opts.StorageQoSIopsMaximum,
			BandwidthMaximum: opts.StorageQoSBandwidthMaximum,
		}
	}

	uvm.scsiLocations[0][0] = &SCSIMount{
		vm:       uvm,
		HostPath: doc.VirtualMachine.Devices.Scsi["0"].Attachments["0"].Path,
		refCount: 1,
	}

	fullDoc, err := mergemaps.MergeJSON(doc, ([]byte)(opts.AdditionHCSDocumentJSON))
	if err != nil {
		return nil, fmt.Errorf("failed to merge additional JSON '%s': %s", opts.AdditionHCSDocumentJSON, err)
	}

	err = uvm.create(ctx, fullDoc)
	if err != nil {
		return nil, fmt.Errorf("error while creating the compute system: %s", err)
	}

	if opts.ExternalGuestConnection {
		log.G(ctx).WithField("vmID", uvm.runtimeID).Debug("Using external GCS bridge")
		l, err := winio.ListenHvsock(&winio.HvsockAddr{
			VMID:      uvm.runtimeID,
			ServiceID: gcs.WindowsGcsHvsockServiceID,
		})
		if err != nil {
			return nil, err
		}
		uvm.gcListener = l
	}

	return uvm, nil
}
