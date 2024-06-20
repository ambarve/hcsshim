//go:build windows

package cim

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Microsoft/go-winio"
	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/Microsoft/hcsshim/internal/oc"
	"github.com/Microsoft/hcsshim/pkg/cimfs"
	"github.com/sirupsen/logrus"
	"go.opencensus.io/trace"
	"golang.org/x/sys/windows"
)

// A BlockCIMLayerWriter implements the wclayer.LayerWriter interface to allow writing container
// image layers in the blocked cim format.
type BlockCIMLayerWriter struct {
	*cimLayerWriter
	// the layer that we are writing
	layer *cimfs.BlockCIM
	// parent layers
	parentLayers []*cimfs.BlockCIM
	// record of all files added so far
	// only added temporarily while we wait for cross layer hard link support
	addedFiles map[string]bool
	// files to delete
	deletedFiles map[string]bool
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

	cim, err := cimfs.CreateBlockCIM(layer.BlockPath, "", layer.CimName, layer.Type)
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
		layer:        layer,
		parentLayers: parentLayers,
		addedFiles:   make(map[string]bool),
		deletedFiles: make(map[string]bool),
		cimLayerWriter: &cimLayerWriter{
			ctx:              ctx,
			cimWriter:        cim,
			stdFileWriter:    sfw,
			layerPath:        filepath.Dir(layer.BlockPath),
			parentLayerPaths: parentLayerPaths,
		},
	}, nil
}

// Add adds a file to the layer with given metadata.
func (cw *BlockCIMLayerWriter) Add(name string, fileInfo *winio.FileBasicInfo, fileSize int64, securityDescriptor []byte, extendedAttributes []byte, reparseData []byte) error {
	if err := cw.cimLayerWriter.Add(name, fileInfo, fileSize, securityDescriptor, extendedAttributes, reparseData); err != nil {
		return err
	}
	cw.addedFiles[name] = true
	return nil
}

// AddLink adds a hard link to the layer. The target must already have been added.
func (cw *BlockCIMLayerWriter) AddLink(name string, target string) error {
	if ok := cw.addedFiles[target]; !ok {
		// pull up the file
		if err := cw.fetchFromParentLayers(target); err != nil {
			return fmt.Errorf("failed to fetch link target: %w", err)
		}
	}
	if err := cw.cimLayerWriter.AddLink(name, target); err != nil {
		return err
	}
	cw.addedFiles[name] = true
	return nil
}

// Remove removes a file that was present in a parent layer from the layer.
func (cw *BlockCIMLayerWriter) Remove(name string) error {
	// set active write to nil so that we panic if layer tar is incorrectly formatted.
	cw.activeWriter = nil
	// TODO(ambarve): ensure that blocked CIMs support storing tombstones here
	cw.deletedFiles[name] = true
	return nil
}

// fetchFromParentLayers looks for the file with `path` in all parent layers one by one and
// if such a file is found, it is added to the layer that this writer is writing
func (cw *BlockCIMLayerWriter) fetchFromParentLayers(path string) error {
	found := false
	for _, c := range cw.parentLayers {
		fileStats, err := cimfs.CIMStatFile(cw.ctx, path, c)
		if err != nil {
			log.G(cw.ctx).WithFields(logrus.Fields{
				"file path": path,
				"cim":       c,
				"error":     err,
			}).Debug("failed to stat file")
			continue
		}

		// file was found, we need to add it to current CIM. However, parent
		// directories of this file may not be present in the current CIM. Add them
		// one by one
		pathElements := strings.Split(path, string(filepath.Separator))
		currPath := ""
		for i := 0; i < len(pathElements)-1; i++ {
			currPath = filepath.Join(currPath, pathElements[i])

			fileBasicInfo := &winio.FileBasicInfo{
				CreationTime:   windows.NsecToFiletime(time.Now().UnixNano()),
				LastAccessTime: windows.NsecToFiletime(time.Now().UnixNano()),
				LastWriteTime:  windows.NsecToFiletime(time.Now().UnixNano()),
				ChangeTime:     windows.NsecToFiletime(time.Now().UnixNano()),
				FileAttributes: windows.FILE_ATTRIBUTE_DIRECTORY,
			}

			if err := cw.Add(currPath, fileBasicInfo, 0, nil, nil, nil); err != nil {
				return fmt.Errorf("failed to add parent dir: %w", err)
			}
		}

		fileBasicInfo := &winio.FileBasicInfo{
			CreationTime:   windows.NsecToFiletime(time.Now().UnixNano()),
			LastAccessTime: windows.NsecToFiletime(time.Now().UnixNano()),
			LastWriteTime:  windows.NsecToFiletime(time.Now().UnixNano()),
			ChangeTime:     windows.NsecToFiletime(time.Now().UnixNano()),
		}

		if err := cw.Add(path, fileBasicInfo, fileStats.EndOfFile, nil, nil, nil); err != nil {
			return fmt.Errorf("failed to add file: %w", err)
		}

		targetReader, err := cimfs.GetCIMFileReader(cw.ctx, path, c)
		if err != nil {
			return fmt.Errorf("failed to get reader: %w", err)
		}
		if _, err = io.Copy(cw, targetReader); err != nil {
			return fmt.Errorf("failed to write file: %w", err)
		}

		found = true
		break
	}
	if !found {
		return fmt.Errorf("couldn't find file %s in parent layers: %w", path, os.ErrNotExist)
	}
	return nil
}

// Close finishes the layer writing process and releases any resources.
func (cw *BlockCIMLayerWriter) Close(ctx context.Context) error {

	parentWriters := []*cimfs.CimFsWriter{}
	for _, c := range cw.parentLayers {
		w, err := cimfs.CreateBlockCIM(c.BlockPath, c.CimName, "", c.Type)
		if err != nil {
			return fmt.Errorf("failed to open parent layer: %w", err)
		}
		parentWriters = append(parentWriters, w)
	}

	for df := range cw.deletedFiles {
		cw.cimWriter.Unlink(df)
		for _, pw := range parentWriters {
			pw.Unlink(df)
		}
	}

	for _, pw := range parentWriters {
		pw.Close()
	}

	return cw.cimLayerWriter.Close(ctx)
}
