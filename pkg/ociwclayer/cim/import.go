package cim

import (
	"archive/tar"
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/Microsoft/go-winio/backuptar"
	"github.com/Microsoft/hcsshim"
	"github.com/Microsoft/hcsshim/internal/wclayer/cim"
	"github.com/Microsoft/hcsshim/pkg/ociwclayer"
	"golang.org/x/sys/windows"
)

// ImportCimLayerFromTar reads a layer from an OCI layer tar stream and extracts it into
// the CIM format at the specified path. The caller must specify the parent layers, if
// any, ordered from lowest to highest layer.
//
// The caller must ensure that the thread or process has acquired backup and
// restore privileges.
//
// This function returns the total size of the layer's files, in bytes.
func ImportCimLayerFromTar(ctx context.Context, r io.Reader, layerPath string, parentLayerPaths []string) (int64, error) {
	err := os.MkdirAll(layerPath, 0)
	if err != nil {
		return 0, err
	}

	home, id := filepath.Split(layerPath)
	info := hcsshim.DriverInfo{
		HomeDir: home,
	}

	w, err := hcsshim.NewCimLayerWriter(info, id, parentLayerPaths)
	if err != nil {
		return 0, err
	}

	n, err := writeCimLayerFromTar(ctx, r, w, layerPath)
	cerr := w.Close(ctx)
	if err != nil {
		return 0, err
	}
	if cerr != nil {
		return 0, cerr
	}
	return n, nil
}

func writeCimLayerFromTar(ctx context.Context, r io.Reader, w *cim.CimLayerWriter, layerPath string) (int64, error) {
	tr := tar.NewReader(r)
	hdr, err := tr.Next()
	buf := bufio.NewWriter(w)
	defer buf.Flush()
	size := int64(0)
	// Iterate through the files in the archive.
	for err == nil {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		// Note: path is used instead of filepath to prevent OS specific handling
		// of the tar path
		base := path.Base(hdr.Name)
		if strings.HasPrefix(base, ociwclayer.WhiteoutPrefix) {
			name := path.Join(path.Dir(hdr.Name), base[len(ociwclayer.WhiteoutPrefix):])
			err = w.Remove(filepath.FromSlash(name))
			if err != nil {
				return 0, err
			}
			hdr, err = tr.Next()
		} else if hdr.Typeflag == tar.TypeLink {
			err = w.AddLink(filepath.FromSlash(hdr.Name), filepath.FromSlash(hdr.Linkname))
			if err != nil {
				return 0, err
			}
			hdr, err = tr.Next()
		} else {
			name, fileSize, fileInfo, err := backuptar.FileInfoFromHeader(hdr)
			if err != nil {
				return 0, err
			}
			sddl, err := backuptar.SecurityDescriptorFromTarHeader(hdr)
			if err != nil {
				return 0, err
			}
			eadata, err := backuptar.ExtendedAttributesFromTarHeader(hdr)
			if err != nil {
				return 0, err
			}
			var reparse []byte
			if hdr.Typeflag == tar.TypeSymlink {
				reparse = backuptar.EncodeReparsePointFromTarHeader(hdr)
				// If reparse point flag is set but reparse buffer is empty remove the flag.
				if (fileInfo.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT) > 0 && len(reparse) == 0 {
					fileInfo.FileAttributes &^= uint32(windows.FILE_ATTRIBUTE_REPARSE_POINT)
				}
			}
			if err := w.Add(filepath.FromSlash(name), fileInfo, fileSize, sddl, eadata, reparse); err != nil {
				return 0, err
			}
			size += fileSize
			if hdr.Typeflag == tar.TypeReg || hdr.Typeflag == tar.TypeRegA {
				_, err = io.Copy(buf, tr)
				if err != nil {
					return 0, err
				}
			}

			// Copy all the alternate data streams and return the next non-ADS header.
			var ahdr *tar.Header
			for {
				ahdr, err = tr.Next()
				if err != nil {
					break
				}

				if ahdr.Typeflag != tar.TypeReg || !strings.HasPrefix(ahdr.Name, hdr.Name+":") {
					hdr = ahdr
					break
				}

				// stream names have following format: '<filename>:<stream name>:$DATA'
				// $DATA is one of the valid types of streams. We currently only support
				// data streams so fail if this is some other type of stream.
				if !strings.HasSuffix(ahdr.Name, ":$DATA") {
					return 0, fmt.Errorf("stream types other than $DATA are not supported, found: %s", ahdr.Name)
				}

				err = w.AddAlternateStream(filepath.FromSlash(ahdr.Name), uint64(ahdr.Size))
				if err != nil {
					return 0, err
				}

				_, err = io.Copy(buf, tr)
				if err != nil {
					return 0, err
				}
			}
		}
		buf.Flush()
	}
	return size, nil
}
