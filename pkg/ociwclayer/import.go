package ociwclayer

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

	winio "github.com/Microsoft/go-winio"
	"github.com/Microsoft/go-winio/backuptar"
	"github.com/Microsoft/hcsshim"
	"github.com/pkg/errors"
	"golang.org/x/sys/windows"
)

const whiteoutPrefix = ".wh."

var (
	// mutatedFiles is a list of files that are mutated by the import process
	// and must be backed up and restored.
	mutatedFiles = map[string]string{
		"UtilityVM/Files/EFI/Microsoft/Boot/BCD":      "bcd.bak",
		"UtilityVM/Files/EFI/Microsoft/Boot/BCD.LOG":  "bcd.log.bak",
		"UtilityVM/Files/EFI/Microsoft/Boot/BCD.LOG1": "bcd.log1.bak",
		"UtilityVM/Files/EFI/Microsoft/Boot/BCD.LOG2": "bcd.log2.bak",
	}
)

// ImportLayerFromTar  reads a layer from an OCI layer tar stream and extracts it to the
// specified path. The caller must specify the parent layers, if any, ordered
// from lowest to highest layer.
//
// The caller must ensure that the thread or process has acquired backup and
// restore privileges.
//
// This function returns the total size of the layer's files, in bytes.
func ImportLayerFromTar(ctx context.Context, r io.Reader, path string, parentLayerPaths []string) (int64, error) {
	err := os.MkdirAll(path, 0)
	if err != nil {
		return 0, err
	}
	w, err := hcsshim.NewLayerWriter(hcsshim.DriverInfo{}, path, parentLayerPaths)
	if err != nil {
		return 0, err
	}
	n, err := writeLayerFromTar(ctx, r, w, path)
	cerr := w.Close()
	if err != nil {
		return 0, err
	}
	if cerr != nil {
		return 0, cerr
	}
	return n, nil
}

func writeLayerFromTar(ctx context.Context, r io.Reader, w hcsshim.LayerWriter, root string) (int64, error) {
	t := tar.NewReader(r)
	hdr, err := t.Next()
	totalSize := int64(0)
	buf := bufio.NewWriter(nil)
	for err == nil {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		base := path.Base(hdr.Name)
		if strings.HasPrefix(base, whiteoutPrefix) {
			name := path.Join(path.Dir(hdr.Name), base[len(whiteoutPrefix):])
			err = w.Remove(filepath.FromSlash(name))
			if err != nil {
				return 0, err
			}
			hdr, err = t.Next()
		} else if hdr.Typeflag == tar.TypeLink {
			err = w.AddLink(filepath.FromSlash(hdr.Name), filepath.FromSlash(hdr.Linkname))
			if err != nil {
				return 0, err
			}
			hdr, err = t.Next()
		} else {
			var (
				name     string
				size     int64
				fileInfo *winio.FileBasicInfo
			)
			name, size, fileInfo, err = backuptar.FileInfoFromHeader(hdr)
			if err != nil {
				return 0, err
			}
			err = w.Add(filepath.FromSlash(name), fileInfo)
			if err != nil {
				return 0, err
			}
			hdr, err = writeBackupStreamFromTarAndSaveMutatedFiles(buf, w, t, hdr, root)
			totalSize += size
		}
	}
	if err != io.EOF {
		return 0, err
	}
	return totalSize, nil
}

// writeBackupStreamFromTarAndSaveMutatedFiles reads data from a tar stream and
// writes it to a backup stream, and also saves any files that will be mutated
// by the import layer process to a backup location.
func writeBackupStreamFromTarAndSaveMutatedFiles(buf *bufio.Writer, w io.Writer, t *tar.Reader, hdr *tar.Header, root string) (nextHdr *tar.Header, err error) {
	var bcdBackup *os.File
	var bcdBackupWriter *winio.BackupFileWriter
	if backupPath, ok := mutatedFiles[hdr.Name]; ok {
		bcdBackup, err = os.Create(filepath.Join(root, backupPath))
		if err != nil {
			return nil, err
		}
		defer func() {
			cerr := bcdBackup.Close()
			if err == nil {
				err = cerr
			}
		}()

		bcdBackupWriter = winio.NewBackupFileWriter(bcdBackup, false)
		defer func() {
			cerr := bcdBackupWriter.Close()
			if err == nil {
				err = cerr
			}
		}()

		buf.Reset(io.MultiWriter(w, bcdBackupWriter))
	} else {
		buf.Reset(w)
	}

	defer func() {
		ferr := buf.Flush()
		if err == nil {
			err = ferr
		}
	}()

	return backuptar.WriteBackupStreamFromTarFile(buf, t, hdr)
}

// ImportCimLayerFromTar reads a layer from an OCI layer tar stream and extracts it to the
// specified path in the CIMFS format. The caller can specify at most one parent
// layer. Note: CIMFS requires that all layers are stored in the same directory. So the
// `path` and path from `parentLayerPaths` should point to the files in the same directory.
//
// The caller must ensure that the thread or process has acquired backup and
// restore privileges.
//
// This function returns the total size of the layer's files, in bytes.
func ImportCimLayerFromTar(ctx context.Context, r io.Reader, path string, parentLayerPaths []string) (size int64, err error) {
	err = os.MkdirAll(path, 0)
	if err != nil {
		return 0, err
	}
	if len(parentLayerPaths) > 0 && filepath.Dir(path) != filepath.Dir(parentLayerPaths[0]) {
		return 0, errors.New("both layer and parent layer paths should be imported to same parent directory")
	}

	info := hcsshim.DriverInfo{
		HomeDir: filepath.Dir(path),
	}
	w, err := hcsshim.NewCimLayerWriter(info, filepath.Base(path), parentLayerPaths)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err2 := w.Close(ctx); err2 != nil {
			if err == nil {
				err = errors.Wrap(err2, "failed to close cim writer")
			}
		}
	}()
	size, err = writeCimLayerFromTar(ctx, r, w, path)
	if err != nil {
		return 0, errors.Wrap(err, "cim layer import failed")
	}
	return
}

// writeCimLayerFromTar applies a tar stream of an OCI style diff tar of a Windows
// layer using the hcsshim cim layer writer.
func writeCimLayerFromTar(ctx context.Context, r io.Reader, w *hcsshim.CimLayerWriter, root string) (int64, error) {
	var size int64
	tr := tar.NewReader(r)
	buf := bufio.NewWriter(w)
	defer buf.Flush()
	hdr, nextErr := tr.Next()
	// Iterate through the files in the archive.
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		if nextErr == io.EOF {
			// end of tar archive
			break
		}
		if nextErr != nil {
			return 0, nextErr
		}

		// Note: path is used instead of filepath to prevent OS specific handling
		// of the tar path
		base := path.Base(hdr.Name)
		if strings.HasPrefix(base, whiteoutPrefix) {
			dir := path.Dir(hdr.Name)
			originalBase := base[len(whiteoutPrefix):]
			originalPath := path.Join(dir, originalBase)
			if err := w.Remove(filepath.FromSlash(originalPath)); err != nil {
				return 0, err
			}
			hdr, nextErr = tr.Next()
		} else if hdr.Typeflag == tar.TypeLink {
			err := w.AddLink(filepath.FromSlash(hdr.Name), filepath.FromSlash(hdr.Linkname))
			if err != nil {
				return 0, err
			}
			hdr, nextErr = tr.Next()
		} else {
			var sddl []byte
			var eadata []byte
			var reparse []byte
			name, fileSize, fileInfo, err := backuptar.FileInfoFromHeader(hdr)
			if err != nil {
				return 0, err
			}
			sddl, err = backuptar.EncodeSDDLFromTarHeader(hdr)
			if err != nil {
				return 0, err
			}
			eadata, err = backuptar.EncodeExtendedAttributesFromTarHeader(hdr)
			if err != nil {
				return 0, err
			}
			if hdr.Typeflag == tar.TypeSymlink {
				reparse = backuptar.EncodeReparsePointFromTarHeader(hdr)
			}
			// If reparse point flag is set but reparse buffer is empty remove the flag.
			if (fileInfo.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT) > 0 && len(reparse) == 0 {
				fileInfo.FileAttributes &^= uint32(windows.FILE_ATTRIBUTE_REPARSE_POINT)
			}
			if err := w.Add(filepath.FromSlash(name), fileInfo, fileSize, sddl, eadata, reparse); err != nil {
				return 0, err
			}
			size += fileSize
			if hdr.Typeflag == tar.TypeReg || hdr.Typeflag == tar.TypeRegA {
				_, err = io.Copy(buf, tr)
				if err != nil {
					return 0, fmt.Errorf("error when copying file data: %s", err)
				}
			}

			// Copy all the alternate data streams and return the next non-ADS header.
			var ahdr *tar.Header
			for {
				ahdr, nextErr = tr.Next()
				if nextErr != nil {
					break
				}
				if ahdr.Typeflag != tar.TypeReg || !strings.HasPrefix(ahdr.Name, hdr.Name+":") {
					hdr = ahdr
					break
				}
				err = w.AddAlternateStream(name, uint64(ahdr.Size))
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
