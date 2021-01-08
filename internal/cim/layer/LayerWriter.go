package layer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Microsoft/go-winio"
	"github.com/Microsoft/go-winio/pkg/security"
	cimfs "github.com/Microsoft/hcsshim/internal/cim/fs"
	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/Microsoft/hcsshim/internal/oc"
	hcsschema "github.com/Microsoft/hcsshim/internal/schema2"
	"github.com/Microsoft/hcsshim/internal/storage"
	"github.com/Microsoft/hcsshim/internal/vhdx"
	"github.com/Microsoft/hcsshim/internal/virtdisk"
	"github.com/Microsoft/hcsshim/internal/wclayer"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
	"golang.org/x/sys/windows"
)

// A CimLayerWriter implements the wclayer.LayerWriter interface to allow writing container
// image layers in the cim format.
// A cim layer consist of cim files (which are usually stored in the `cim-layers` directory and
// some other files which are stored in the directory of that layer (i.e the `path` directory).
type CimLayerWriter struct {
	ctx context.Context
	s   *trace.Span
	// path to the layer (i.e layer's directory) as provided by the caller.
	// Even if a layer is stored as a cim in the cim directory, some files associated
	// with a layer are still stored in this path.
	path string
	// parent layer paths
	parentLayerPaths []string
	// Handle to the layer cim - writes to the cim file
	cimWriter *cimfs.CimFsWriter
	// Handle to the writer for writing files in the local filesystem
	stdFileWriter *stdFileWriter
	// reference to currently active writer either cimWriter or stdFileWriter
	activeWriter io.Writer
}

const (
	regFilesPath        = "Files\\Windows\\System32\\config"
	hivesPath           = "Hives"
	utilityVMPath       = "UtilityVM"
	utilityVMFilesPath  = "UtilityVM\\Files"
	bcdFilePath         = "UtilityVM\\Files\\EFI\\Microsoft\\Boot\\BCD"
	containerBaseVhd    = "blank-base.vhdx"
	containerScratchVhd = "blank.vhdx"
	utilityVMBaseVhd    = "SystemTemplateBase.vhdx"
	utilityVMScratchVhd = "SystemTemplate.vhdx"
	layoutFileName      = "layout"
)

type hive struct {
	name  string
	base  string
	delta string
}

var (
	hives = []hive{
		{"SYSTEM", "SYSTEM_BASE", "SYSTEM_DELTA"},
		{"SOFTWARE", "SOFTWARE_BASE", "SOFTWARE_DELTA"},
		{"SAM", "SAM_BASE", "SAM_DELTA"},
		{"SECURITY", "SECURITY_BASE", "SECURITY_DELTA"},
		{"DEFAULT", "DEFAULTUSER_BASE", "DEFAULTUSER_DELTA"},
	}
)

func isDeltaHive(path string) bool {
	for _, hv := range hives {
		if strings.EqualFold(filepath.Base(path), hv.delta) {
			return true
		}
	}
	return false
}

// checks if this particular file should be written with a stdFileWriter instead of
// using the cimWriter.
func isStdFile(path string) bool {
	return (isDeltaHive(path) || path == bcdFilePath)
}

// Add adds a file to the layer with given metadata.
func (cw *CimLayerWriter) Add(name string, fileInfo *winio.FileBasicInfo, fileSize int64, securityDescriptor []byte, extendedAttributes []byte, reparseData []byte) error {
	if isStdFile(name) {
		if err := cw.stdFileWriter.Add(name); err != nil {
			return err
		}
		cw.activeWriter = cw.stdFileWriter
	} else {
		if err := cw.cimWriter.AddFile(name, fileInfo, fileSize, securityDescriptor, extendedAttributes, reparseData); err != nil {
			return err
		}
		cw.activeWriter = cw.cimWriter
	}
	return nil
}

// AddLink adds a hard link to the layer. The target must already have been added.
func (cw *CimLayerWriter) AddLink(name string, target string) error {
	if isStdFile(name) {
		return cw.stdFileWriter.AddLink(name, target)
	} else {
		return cw.cimWriter.AddLink(target, name)
	}
}

// AddAlternateStream creates another alternate stream at the given
// path. Any writes made after this call will go to that stream.
func (cw *CimLayerWriter) AddAlternateStream(name string, size uint64) error {
	if isStdFile(name) {
		if err := cw.stdFileWriter.Add(name); err != nil {
			return err
		}
		cw.activeWriter = cw.stdFileWriter
	} else {
		if err := cw.cimWriter.CreateAlternateStream(name, size); err != nil {
			return err
		}
		cw.activeWriter = cw.cimWriter
	}
	return nil
}

// Remove removes a file that was present in a parent layer from the layer.
func (cw *CimLayerWriter) Remove(name string) error {
	if isStdFile(name) {
		return cw.stdFileWriter.Remove(name)
	} else {
		return cw.cimWriter.Unlink(name)
	}
}

// Write writes data to the current file. The data must be in the format of a Win32
// backup stream.
func (cw *CimLayerWriter) Write(b []byte) (int, error) {
	return cw.activeWriter.Write(b)
}

// baseVhdHandle must be a valid open handle to a vhd if this is a layer of type hcsschema.VmLayer
// If this is a layer of type hcsschema.ContainerLayer then handle is ignored.
func setupBaseLayer(ctx context.Context, baseVhdHandle windows.Handle, layerPath string, layerType hcsschema.OsLayerType) error {
	layerOptions := hcsschema.OsLayerOptions{
		Type_:                      layerType,
		DisableCiCacheOptimization: true,
		SkipUpdateBcdForBoot:       (layerType == hcsschema.VmLayer),
	}

	if layerType == hcsschema.ContainerLayer {
		baseVhdHandle = 0
	}

	layerOptionsJson, err := json.Marshal(layerOptions)
	if err != nil {
		return fmt.Errorf("failed to marshal layer options: %s", err)
	}

	if err := storage.SetupBaseOSLayer(ctx, layerPath, baseVhdHandle, string(layerOptionsJson)); err != nil {
		return fmt.Errorf("failed to setup base os layer: %s", err)
	}

	return nil
}

func createDiffVhd(ctx context.Context, diffVhdPath, baseVhdPath string) error {
	// create the differencing disk
	createParams := &virtdisk.CreateVirtualDiskParameters{
		Version: 2,
		Version2: virtdisk.CreateVersion2{
			ParentPath:       windows.StringToUTF16Ptr(baseVhdPath),
			BlockSizeInBytes: 1 * 1024 * 1024,
			OpenFlags:        uint32(virtdisk.OpenVirtualDiskFlagCachedIO),
		},
	}

	vhdHandle, err := virtdisk.CreateVirtualDisk(ctx, diffVhdPath, virtdisk.VirtualDiskAccessFlagNone, virtdisk.CreateVirtualDiskFlagNone, createParams)
	if err != nil {
		return fmt.Errorf("failed to create differencing vhd: %s", err)
	}
	if err := windows.CloseHandle(vhdHandle); err != nil {
		return fmt.Errorf("failed to close differencing vhd handle: %s", err)
	}
	return nil
}

// TODO(ambarve): Danny has already created a PR to add all of the new HCS storage APIs.
// rebase with that PR instead
func setupContainerBaseLayer(ctx context.Context, layerPath string) error {
	baseVhdPath := filepath.Join(layerPath, containerBaseVhd)
	diffVhdPath := filepath.Join(layerPath, containerScratchVhd)

	createParams := &virtdisk.CreateVirtualDiskParameters{
		Version: 2,
		Version2: virtdisk.CreateVersion2{
			MaximumSize:      uint64(20) * 1024 * 1024 * 1024,
			BlockSizeInBytes: 1 * 1024 * 1024,
		},
	}

	handle, err := virtdisk.CreateVirtualDisk(ctx, baseVhdPath, virtdisk.VirtualDiskAccessFlagNone, virtdisk.CreateVirtualDiskFlagNone, createParams)
	if err != nil {
		return fmt.Errorf("failed to create VHD: %s", err)
	}

	if err = storage.FormatWritableLayerVhd(ctx, handle); err != nil {
		return errors.Wrap(err, "failed to format VHD")
	}

	// base vhd handle must be closed before calling SetupBaseLayer in case of Container layer
	if err = windows.CloseHandle(handle); err != nil {
		return fmt.Errorf("failed to close VHD handle : %s", err)
	}

	if err = setupBaseLayer(ctx, handle, layerPath, hcsschema.ContainerLayer); err != nil {
		return fmt.Errorf("failed to setup container base layer: %s", err)
	}

	if err = createDiffVhd(ctx, diffVhdPath, baseVhdPath); err != nil {
		return err
	}

	if err := security.GrantVmGroupAccess(baseVhdPath); err != nil {
		return fmt.Errorf("failed to grant vm group access to %s: %s", baseVhdPath, err)
	}

	if err := security.GrantVmGroupAccess(diffVhdPath); err != nil {
		return fmt.Errorf("failed to grant vm group access to %s: %s", diffVhdPath, err)
	}
	return nil
}

// `layerPath` must be the path at which all of the layer files can be accessed and `vhdCreationPath` must
// a path inside which the vhd files for uvm will be created.
func setupUtilityVMBaseLayer(ctx context.Context, layerPath, vhdCreationPath string) error {
	baseVhdPath := filepath.Join(vhdCreationPath, utilityVMPath, utilityVMBaseVhd)
	diffVhdPath := filepath.Join(vhdCreationPath, utilityVMPath, utilityVMScratchVhd)

	// Just create the vhd for utilityVM layer, no need to format it.
	createParams := &virtdisk.CreateVirtualDiskParameters{
		Version: 2,
		Version2: virtdisk.CreateVersion2{
			MaximumSize:      uint64(10) * 1024 * 1024 * 1024,
			BlockSizeInBytes: 1 * 1024 * 1024,
		},
	}

	handle, err := virtdisk.CreateVirtualDisk(ctx, baseVhdPath, virtdisk.VirtualDiskAccessFlagNone, virtdisk.CreateVirtualDiskFlagNone, createParams)
	if err != nil {
		return fmt.Errorf("failed to create VHD: %s", err)
	}

	// If it is a utilityVM layer then the base vhd must be attached when calling
	// SetupBaseOSLayer
	attachParams := &virtdisk.AttachVirtualDiskParameters{
		Version: 2,
	}
	err = virtdisk.AttachVirtualDisk(ctx, handle, virtdisk.AttachVirtualDiskFlagNone, attachParams)
	if err != nil {
		return err
	}

	if err = setupBaseLayer(ctx, handle, layerPath, hcsschema.VmLayer); err != nil {
		return fmt.Errorf("failed to setup utility vm base layer: %s", err)
	}

	if err = virtdisk.DetachVirtualDisk(ctx, handle); err != nil {
		return fmt.Errorf("failed to detach VHD: %s", err)
	}

	if err = windows.CloseHandle(handle); err != nil {
		return fmt.Errorf("failed to close VHD handle: %s", err)
	}

	partitionInfo, err := vhdx.GetScratchVhdPartitionInfo(ctx, baseVhdPath)
	if err != nil {
		return fmt.Errorf("failed to get base vhd layout info: %s", err)
	}

	if err := updateBcdStoreForBoot(filepath.Join(vhdCreationPath, bcdFilePath), partitionInfo.DiskID, partitionInfo.PartitionID); err != nil {
		return fmt.Errorf("failed to update BCD: %s", err)
	}

	// Note: diff vhd creation and granting of vm group access must be done AFTER
	// getting the partition info of the base VHD. Otherwise it causes the vhd parent
	// chain to get corrupted.
	if err = createDiffVhd(ctx, diffVhdPath, baseVhdPath); err != nil {
		return err
	}

	if err := security.GrantVmGroupAccess(baseVhdPath); err != nil {
		return fmt.Errorf("failed to grant vm group access to %s: %s", baseVhdPath, err)
	}

	if err := security.GrantVmGroupAccess(diffVhdPath); err != nil {
		return fmt.Errorf("failed to grant vm group access to %s: %s", diffVhdPath, err)
	}

	return nil
}

// Close finishes the layer writing process and releases any resources.
func (cw *CimLayerWriter) Close(ctx context.Context) (err error) {
	if err := cw.stdFileWriter.Close(ctx); err != nil {
		return err
	}

	// cimWriter must be closed before doing any further processing on this layer.
	if err := cw.cimWriter.Close(); err != nil {
		return err
	}

	if len(cw.parentLayerPaths) == 0 {
		if err := processBaseLayer(ctx, cw.path); err != nil {
			return fmt.Errorf("processBaseLayer failed: %s", err)
		}

		if err := postProcessBaseLayer(ctx, cw.path); err != nil {
			return fmt.Errorf("postProcessBaseLayer failed: %s", err)
		}
	} else {
		if err := processNonBaseLayer(ctx, cw.path, cw.parentLayerPaths); err != nil {
			return fmt.Errorf("failed to process layer: %s", err)
		}
	}

	// TODO(ambarve): Add a failure if we see files inside UtilityVM directory in a non-base layer.

	return nil
}

func NewCimLayerWriter(ctx context.Context, path string, parentLayerPaths []string) (_ *CimLayerWriter, err error) {
	ctx, span := trace.StartSpan(ctx, "hcsshim::NewCimLayerWriter")
	defer func() {
		if err != nil {
			oc.SetSpanStatus(span, err)
			span.End()
		}
	}()
	span.AddAttributes(
		trace.StringAttribute("path", path),
		trace.StringAttribute("parentLayerPaths", strings.Join(parentLayerPaths, ", ")))

	parentCim := ""
	cimDirPath := GetCimDirFromLayer(path)
	if _, err = os.Stat(cimDirPath); os.IsNotExist(err) {
		// create cim directory
		if err = os.Mkdir(cimDirPath, 0755); err != nil {
			return nil, fmt.Errorf("failed while creating cim layers directory: %s", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("unable to access cim layers directory: %s", err)

	}

	if len(parentLayerPaths) > 0 {
		parentCim = GetCimNameFromLayer(parentLayerPaths[0])
	}

	cim, err := cimfs.Create(GetCimDirFromLayer(path), parentCim, GetCimNameFromLayer(path))
	if err != nil {
		return nil, fmt.Errorf("error in creating a new cim: %s", err)
	}

	sfw, err := newStdFileWriter(path, parentLayerPaths)
	if err != nil {
		return nil, fmt.Errorf("error in creating new standard file writer: %s", err)
	}
	return &CimLayerWriter{
		ctx:              ctx,
		s:                span,
		path:             path,
		parentLayerPaths: parentLayerPaths,
		cimWriter:        cim,
		stdFileWriter:    sfw,
	}, nil
}

func DestroyCimLayer(ctx context.Context, layerPath string) error {
	// This layer could be a container / sandbox layer and not an image layer and
	// so might not have the cim files. Simply forward the call to DestroyLayer HCS API
	// in that case.
	if err := wclayer.DestroyLayer(ctx, layerPath); err != nil {
		return err
	}

	// containerd renames the layer directory from `<layerID>` to `rm-<layerID>` before
	// calling destroy layer on it. So here first we need to get the original layerID from
	// the layerPath by removing the `rm` prefix.
	// Probably there is a cleaner way to do this? Ideally if we keep the cim files in the
	// layer folders then we won't have to worry about this at all. But that is not possible
	// at the moment.
	originalLayerId := strings.TrimPrefix(filepath.Base(layerPath), "rm-")
	// Note that the originalLayerPath doesn't exist at this point. We just create this string
	// to get the cimPath.
	originalLayerPath := filepath.Join(filepath.Dir(layerPath), originalLayerId)
	cimPath := GetCimPathFromLayer(originalLayerPath)
	log.G(ctx).Debugf("DestroyCimLayer layerPath: %s, cimPath: %s", layerPath, cimPath)
	if _, err := os.Stat(cimPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return cimfs.DestroyCim(cimPath)
}
