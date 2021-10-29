package cim

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/Microsoft/go-winio"
	"github.com/Microsoft/hcsshim/internal/cimfs"
	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/Microsoft/hcsshim/internal/mylogger"
	"github.com/Microsoft/hcsshim/internal/oc"
	"github.com/Microsoft/hcsshim/internal/wclayer"
	"go.opencensus.io/trace"
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
	// denotes if this layer has the UtilityVM directory
	hasUtilityVM bool
}

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

const bootmgFile = `UtilityVM\Files\EFI\Microsoft\Boot\bootmgfw.efi`

// checks if this particular file should be written with a stdFileWriter instead of
// using the cimWriter.
func isStdFile(path string) bool {
	return (isDeltaHive(path) || path == wclayer.BcdFilePath || path == bootmgFile)
	// return (isDeltaHive(path) || path == wclayer.BcdFilePath)
}

// Add adds a file to the layer with given metadata.
func (cw *CimLayerWriter) Add(name string, fileInfo *winio.FileBasicInfo, fileSize int64, securityDescriptor []byte, extendedAttributes []byte, reparseData []byte) error {
	if name == wclayer.UtilityVMPath {
		cw.hasUtilityVM = true
	}
	if strings.Contains(name, "ntoskrnl.exe") {
		mylogger.LogFmt("kernel found at %s\n", name)
	}

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

func execWithPowershell(args ...string) error {
	var out bytes.Buffer
	cmd := exec.Command("powershell.exe", args...)
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		fmt.Printf("execWithPowershell (%s) failed with error: %s, stdout: %s\n", cmd.String(), err, out.String())
		return err
	}
	fmt.Println(out.String())
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
		if err := processBaseLayer(ctx, cw.path, cw.hasUtilityVM); err != nil {
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

	// mount the vhd and copy cim to that
	cimVhdPath := "D:\\Containers\\testdata\\cimboot\\sandbox.vhdx"

	if err = execWithPowershell("Mount-VHD", cimVhdPath); err != nil {
		return fmt.Errorf("mount vhd failed : %s", err)
	}
	defer execWithPowershell("Dismount-VHD", cimVhdPath)

	cimlayersPath := GetCimDirFromLayer(cw.path)
	if err = execWithPowershell("rm", "-r", "F:/cim-layers/1/*"); err != nil {
		return fmt.Errorf("rm from vhd failed : %s", err)
	}
	if err = execWithPowershell("cp", cimlayersPath+"/*", "F:/cim-layers/1/"); err != nil {
		return fmt.Errorf("copy cim to vhd failed : %s", err)
	}
	if err = execWithPowershell("cp", "\\\\winbuilds\\release\\rs_fun_deploy_t3\\22486.1000.211023-1934\\amd64fre\\bin\\bootmgrasbuilt\\bootmgfw.efi", filepath.Join(cw.path, "UtilityVM\\Files\\EFI\\Microsoft\\Boot")); err != nil {
		return fmt.Errorf("copy new bootmgw.efi failed : %s", err)
	}

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

// DestroyCimLayer destroys a cim layer i.e it removes all the cimfs files for the given layer as well as
// all of the other files that are stored in the layer directory (at path `layerPath`).
// If this is not a cimfs layer (i.e a cim file for the given layer does not exist) then nothing is done.
func DestroyCimLayer(ctx context.Context, layerPath string) error {
	cimPath := GetCimPathFromLayer(layerPath)

	// verify that such a cim exists first, sometimes containerd tries to call
	// this with the root snapshot directory as the layer path. We don't want to
	// destroy everything inside the snapshots directory.
	log.G(ctx).Debugf("DestroyCimLayer layerPath: %s, cimPath: %s", layerPath, cimPath)
	if _, err := os.Stat(cimPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if err := wclayer.DestroyLayer(ctx, layerPath); err != nil {
		return err
	}
	return cimfs.DestroyCim(cimPath)
}
