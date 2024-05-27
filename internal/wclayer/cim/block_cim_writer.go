//go:build windows

package cim

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/Microsoft/hcsshim/internal/oc"
	"github.com/Microsoft/hcsshim/pkg/cimfs"
	"github.com/sirupsen/logrus"
	"go.opencensus.io/trace"
)

// A BlockCIMLayerWriter implements the wclayer.LayerWriter interface to allow writing container
// image layers in the blocked cim format.
type BlockCIMLayerWriter struct {
	*cimLayerWriter
	// the layer that we are writing
	layer *cimfs.BlockCIM
	// parent layers
	parentLayers []*cimfs.BlockCIM
}

var _ CIMLayerWriter = &BlockCIMLayerWriter{}

// NewBlockCIMLayerWriter writes the layer files in the block CIM format.
func NewBlockCIMLayerWriter(ctx context.Context, layer *cimfs.BlockCIM, parentLayers []*cimfs.BlockCIM) (_ *BlockCIMLayerWriter, err error) {
	if !cimfs.IsBlockedCimSupported() {
		return nil, fmt.Errorf("BlockCIM not supported on this build")
	} else if layer.Type != cimfs.BlockCIMTypeSingleFile {
		// we only support writing single file CIMs for now because in layer
		// writing process we still need to write some files (registry hives)
		// outside the CIM in the directory where the single file block CIM is
		// stored. This can't be reliably done with the block device CIM since the
		// block path provided will be a volume path. However, once we get rid of
		// hive rollup step during layer import we should be able to support block
		// device CIMs.
		return nil, ErrBlockCIMWriterNotSupported
	}

	ctx, span := trace.StartSpan(ctx, "hcsshim::NewBlockCIMLayerWriter")
	defer func() {
		if err != nil {
			oc.SetSpanStatus(span, err)
			span.End()
		}
	}()
	span.AddAttributes(
		trace.StringAttribute("layer", layer.String()))

	parentLayerPaths := make([]string, 0, len(parentLayers))
	for _, pl := range parentLayers {
		if pl.Type != layer.Type {
			return nil, ErrBlockCIMParentTypeMismatch
		}
		parentLayerPaths = append(parentLayerPaths, filepath.Dir(pl.BlockPath))
	}

	cim, err := cimfs.CreateBlockCIM(layer.BlockPath, layer.CimName, layer.Type)
	if err != nil {
		return nil, fmt.Errorf("error in creating a new cim: %w", err)
	}
	log.G(ctx).WithFields(logrus.Fields{
		"layer": layer,
	}).Info("created new block CIM")

	// std file writer writes registry hives outside the CIM for 2 reasons.  1. We can
	// merge the hives of this layer with the parent layer hives and then write the
	// merged hives into the CIM.  2. When importing child layer of this layer, we
	// have access to the merges hives of this layer.
	sfw, err := newStdFileWriter(filepath.Dir(layer.BlockPath), parentLayerPaths)
	if err != nil {
		return nil, fmt.Errorf("error in creating new standard file writer: %w", err)
	}

	return &BlockCIMLayerWriter{
		cimLayerWriter: &cimLayerWriter{
			ctx:              ctx,
			cimWriter:        cim,
			stdFileWriter:    sfw,
			layerPath:        filepath.Dir(layer.BlockPath),
			parentLayerPaths: parentLayerPaths,
		},
	}, nil
}

// Remove removes a file that was present in a parent layer from the layer.
func (cw *BlockCIMLayerWriter) Remove(name string) error {
	// set active write to nil so that we panic if layer tar is incorrectly formatted.
	cw.activeWriter = nil
	// TODO(ambarve): ensure that blocked CIMs support storing tombstones here
	err := cw.cimWriter.Unlink(name)
	if err != nil {
		return fmt.Errorf("failed to remove file : %w", err)
	}
	return nil
}
