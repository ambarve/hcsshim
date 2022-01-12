package wclayer

import (
	"context"
	"path/filepath"
	"strings"

	winio "github.com/Microsoft/go-winio"
	"github.com/Microsoft/hcsshim/internal/hcserror"
	"github.com/Microsoft/hcsshim/internal/oc"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
)

// FileInfoProvider provides all kinds of information about a particular layer file.  The
// layer files aren't always stored in plain NTFS file format (some times they are stored
// in other format like CIMFS or a VHD etc.). In such cases layer management functions
// can't directly read those files and their metadata (like SDDL, EA etc.). To handle such
// cases, for each layer storage format we provide an implementations of this interface
// that can understand the underlying layer and provide required information.
type LayerFileInfoProvider interface {
	GetFileBasicInformation(path string) (*winio.FileBasicInfo, error)
	GetFileStandardInformation(path string) (*winio.FileStandardInfo, error)
}

// LayerWalkFunc is a handler called by LayerWalker for every file entry as it walks the
// layer file tree. `path` is the path to a specific file under the layer and `fiProvider` is
// the information provider for that specific file.
type LayerWalkFunc func(ctx context.Context, path string, fiProvider LayerFileInfoProvider) error

// fsWalker is a walker for a layer filesystem tree. It is similar to filepath.Walk but
// designed for walking layer filesystems. Layer file systems aren't always stored in
// plain NTFS format (plus many times they can be overlayed on top of each other). So we
// can't use the standard filepath.Walk.
type LayerWalker interface {
	Walk(handler LayerWalkFunc) error
}

// initializeSandboxStateDirectory creates the sandbox state directory at the root of the
// sandbox VHD.  `scratchRoot` should point to the volume at which the VHD is mounted on
// the host.
func initializeSandboxStateDirectory(scratchRoot string) error {
	return errors.New("Not implemented")
}

func prepareScratch(ctx context.Context, scratchPath string, walker LayerWalker) error {
	// Mount layer VHD
	// TODO(ambarve): We should replace these legacy HCS API calls by mounting the VHD by ourselves.
	if err := ActivateLayer(ctx, filepath.Dir(scratchPath)); err != nil {
		return err
	}
	defer DeactivateLayer(ctx, filepath.Dir(scratchPath))
	mountPath, err := GetLayerMountPath(ctx, filepath.Dir(scratchPath))
	if err != nil {
		return err
	}
	// initialize sandbox state directory

	// take process privileges
	err = winio.EnableProcessPrivileges([]string{winio.SeBackupPrivilege, winio.SeRestorePrivilege})
	if err != nil {
		return err
	}

	// expand wci reparse points by traversing the layer tree.
	wc := &wcifsReparsePointCreator{
		targetPath: mountPath,
	}
	walker.Walk(wc.createWciReparsePoint)

	// do the special handling for ARM64, if required
	return nil
}

// CreateScratchLayer creates and populates new read-write layer for use by a container.
// This requires the full list of paths to all parent layers up to the base
func CreateScratchLayer(ctx context.Context, path string, parentLayerPaths []string) (err error) {
	title := "hcsshim::CreateScratchLayer"
	ctx, span := trace.StartSpan(ctx, title)
	defer span.End()
	defer func() { oc.SetSpanStatus(span, err) }()
	span.AddAttributes(
		trace.StringAttribute("path", path),
		trace.StringAttribute("parentLayerPaths", strings.Join(parentLayerPaths, ", ")))

	// Generate layer descriptors
	layers, err := layerPathsToDescriptors(ctx, parentLayerPaths)
	if err != nil {
		return err
	}

	err = createSandboxLayer(&stdDriverInfo, path, 0, layers)
	if err != nil {
		return hcserror.New(err, title, "")
	}
	return nil
}
