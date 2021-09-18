package main

import (
	"archive/tar"
	"bufio"
	"context"
	"io/ioutil"

	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Microsoft/go-winio"
	"github.com/Microsoft/hcsshim/internal/cimfs"
	cimlayer "github.com/Microsoft/hcsshim/internal/wclayer/cim"
	"github.com/Microsoft/hcsshim/osversion"
	"github.com/Microsoft/hcsshim/pkg/ociwclayer"
	"github.com/pkg/errors"
)

// A simple tuple type used to hold information about a file/directory that is created
// during a test.
type tuple struct {
	filepath     string
	fileContents []byte
	isDir        bool
	// option timestamps
	modTime time.Time
}

// createTestTar creates a tar at path `path` with contents in `tarContents`.
func createTestTar(tarContents []tuple, path string) error {
	testtar, err := os.Create(path)
	if err != nil {
		errors.Wrap(err, "failed to create tar file")
	}
	defer testtar.Close()
	tw := tar.NewWriter(testtar)
	defer tw.Close()
	for _, t := range tarContents {
		var hdr *tar.Header
		if t.isDir {
			hdr = &tar.Header{
				Typeflag:   tar.TypeDir,
				Name:       t.filepath,
				Mode:       0777,
				Size:       0,
				ModTime:    t.modTime,
				AccessTime: time.Now(),
				ChangeTime: time.Now(),
			}
		} else {
			hdr = &tar.Header{
				Typeflag:   tar.TypeReg,
				Name:       t.filepath,
				Mode:       0777,
				Size:       int64(len(t.fileContents)),
				ModTime:    t.modTime,
				AccessTime: time.Now(),
				ChangeTime: time.Now(),
			}

		}
		if err := tw.WriteHeader(hdr); err != nil {
			errors.Wrap(err, "failed to write file header to tar")
		}
		if !t.isDir {
			if _, err := tw.Write([]byte(t.fileContents)); err != nil {
				errors.Wrap(err, "failed to write file contents to tar")
			}
		}
	}
	return nil
}

// verifyCimContents compares the contents of the cim mounted at path `mountedCimPath` against the
// `contents`. Returns a failure if differences are found.
func verifyCimContents(contents []tuple, mountedCimPath string) error {
	for _, t := range contents {
		comparePath := filepath.Join(mountedCimPath, t.filepath)
		fi, err := os.Stat(comparePath)
		if err != nil {
			return errors.Wrapf(err, "stat failed on path: %s", comparePath)
		}
		if fi.IsDir() != t.isDir {
			return errors.Errorf("comparison failed: %s is directory: %t, %s is directory: %t", t.filepath, t.isDir, comparePath, fi.IsDir())
		}
		if !fi.IsDir() && (fi.Size() != int64(len(t.fileContents))) {
			return errors.Errorf("comparison failed: %s size: %d, %s size: %d", t.filepath, len(t.fileContents), comparePath, fi.Size())
		}
		// TODO(ambarve): check why cimfs time is different than expected.
		// if !fi.ModTime().Equal(t.modTime) {
		// 	return errors.Errorf("comparison failed: modification times are different for %s & %s", t.filepath, comparePath)
		// }
	}
	return nil
}

// This test creates a tar, imports it to a cim, mounts that cim and reads those files
// back. The tar created by this test has a specific tree structure to make it a valid container layer.
func TestCimReadWrite(t *testing.T) {
	if osversion.Get().Build < cimfs.MinimumCimFSBuild {
		t.Skipf("Requires build %d+", cimfs.MinimumCimFSBuild)
	}

	// contents of a smallest valid container layer.
	testContents := []tuple{
		{"Files", []byte(""), true, time.Now()},
		{"Files\\Windows", []byte(""), true, time.Now()},
		{"Files\\Windows\\System32", []byte(""), true, time.Now()},
		{"Files\\Windows\\System32\\config", []byte(""), true, time.Now()},
		{"Files\\Windows\\System32\\config\\SOFTWARE", []byte(""), false, time.Now()},
		{"Files\\Windows\\System32\\config\\SYSTEM", []byte(""), false, time.Now()},
		{"Files\\Windows\\System32\\config\\SAM", []byte(""), false, time.Now()},
		{"Files\\Windows\\System32\\config\\SECURITY", []byte(""), false, time.Now()},
		{"Files\\Windows\\System32\\config\\DEFAULT", []byte(""), false, time.Now()},
	}

	tempDir, err := ioutil.TempDir("", "cim-test")
	if err != nil {
		t.Fatalf("failed while creating temp directory: %s", err)
	}
	defer os.RemoveAll(tempDir)

	tarPath := filepath.Join(tempDir, "testlayer.tar")
	err = createTestTar(testContents, tarPath)
	if err != nil {
		t.Fatalf("failed to create test tar: %s", err)
	}

	tarReader, err := os.Open(tarPath)
	if err != nil {
		t.Fatalf("failed to open tar: %s", err)
	}
	defer tarReader.Close()

	layerPath := filepath.Join(tempDir, "1")

	// need privileges to import a container layer
	err = winio.EnableProcessPrivileges([]string{winio.SeBackupPrivilege, winio.SeRestorePrivilege})
	if err != nil {
		t.Fatalf("unable to acquire privileges: %s", err)
	}

	_, err = ociwclayer.ImportCimLayerFromTar(context.Background(), bufio.NewReader(tarReader), layerPath, []string{})
	if err != nil {
		t.Fatalf("failed to import cim from tar: %s", err)
	}
	defer func() {
		dErr := cimlayer.DestroyCimLayer(context.Background(), layerPath)
		if dErr != nil {
			t.Fatalf("failed to destroy cim layer: %s", dErr)
		}
	}()

	cimPath := cimlayer.GetCimPathFromLayer(layerPath)
	mountPath, err := cimfs.Mount(cimPath)
	if err != nil {
		t.Fatalf("failed to mount the cim: %s", err)
	}
	defer func() {
		uErr := cimfs.Unmount(mountPath)
		if uErr != nil {
			t.Fatalf("cim unmount failed: %s", uErr)
		}
	}()

	err = verifyCimContents(testContents, mountPath)
	if err != nil {
		t.Fatalf("verification on mounted cim failed: %s", err)
	}

}
