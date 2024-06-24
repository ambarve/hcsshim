//go:build windows

package cim

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Microsoft/hcsshim/internal/oc"
	"github.com/Microsoft/hcsshim/pkg/cimfs"
	"go.opencensus.io/trace"
)

// A ForkedCimLayerWriter implements the wclayer.LayerWriter interface to allow writing container
// image layers in the cim format.
// A cim layer consist of cim files (which are usually stored in the `cim-layers` directory and
// some other files which are stored in the directory of that layer (i.e the `path` directory).
type ForkedCimLayerWriter struct {
	*cimLayerWriter
}

var _ CIMLayerWriter = &ForkedCimLayerWriter{}

func NewForkedCimLayerWriter(ctx context.Context, layerPath, cimPath string, parentLayerPaths, parentLayerCimPaths []string) (_ *ForkedCimLayerWriter, err error) {
	if !cimfs.IsCimFSSupported() {
		return nil, fmt.Errorf("CimFs not supported on this build")
	}

	ctx, span := trace.StartSpan(ctx, "hcsshim::NewCimLayerWriter")
	defer func() {
		if err != nil {
			oc.SetSpanStatus(span, err)
			span.End()
		}
	}()
	span.AddAttributes(
		trace.StringAttribute("path", layerPath),
		trace.StringAttribute("cimPath", cimPath),
		trace.StringAttribute("parentLayerPaths", strings.Join(parentLayerCimPaths, ", ")),
		trace.StringAttribute("parentLayerPaths", strings.Join(parentLayerPaths, ", ")))

	parentCim := ""
	if len(parentLayerPaths) > 0 {
		// We only need to provide parent CIM name, it is assumed that both parent CIM
		// and newly created CIM are present in the same directory.
		parentCim = filepath.Base(parentLayerCimPaths[0])
	}

	cim, err := cimfs.Create(filepath.Dir(cimPath), parentCim, filepath.Base(cimPath))
	if err != nil {
		return nil, fmt.Errorf("error in creating a new cim: %w", err)
	}

	sfw, err := newStdFileWriter(layerPath, parentLayerPaths)
	if err != nil {
		return nil, fmt.Errorf("error in creating new standard file writer: %w", err)
	}
	return &ForkedCimLayerWriter{
		cimLayerWriter: &cimLayerWriter{
			parentLayerPaths: parentLayerPaths,
			ctx:              ctx,
			cimWriter:        cim,
			stdFileWriter:    sfw,
			layerPath:        layerPath,
		},
	}, nil
}

// Remove removes a file that was present in a parent layer from the layer.
func (cw *ForkedCimLayerWriter) Remove(name string) error {
	// set active write to nil so that we panic if layer tar is incorrectly formatted.
	cw.activeWriter = nil
	err := cw.cimWriter.Unlink(name)
	if err == nil || os.IsNotExist(err) {
		return nil
	}
	return fmt.Errorf("failed to remove file: %w", err)
}
