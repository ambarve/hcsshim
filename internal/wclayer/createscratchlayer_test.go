package wclayer

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"testing"

	winio "github.com/Microsoft/go-winio"
	"github.com/Microsoft/go-winio/vhd"
	"github.com/Microsoft/hcsshim/computestorage"
	"github.com/pkg/errors"
	"golang.org/x/sys/windows"
)

type legacyLayerWalker struct {
	root string
}

func (l *legacyLayerWalker) Walk(handler LayerWalkFunc) error {
	stdFi := &stdFileInfoProvider{
		root: l.root,
	}
	return filepath.WalkDir(l.root, func(path string, dirEntry fs.DirEntry, err error) error {
		if err == nil {
			fmt.Printf("walk file: %s\n", path)
			return handler(context.TODO(), path, stdFi)
		}
		return err
	})
}

func createTestVhdx(vhdPath string) error {
	createParams := &vhd.CreateVirtualDiskParameters{
		Version: 2,
		Version2: vhd.CreateVersion2{
			MaximumSize:      10 * 1024 * 1024 * 1024,
			BlockSizeInBytes: 1 * 1024 * 1024,
		},
	}
	handle, err := vhd.CreateVirtualDisk(vhdPath, vhd.VirtualDiskAccessNone, vhd.CreateVirtualDiskFlagNone, createParams)
	if err != nil {
		return errors.Wrap(err, "failed to create vhdx")
	}
	defer windows.Close(windows.Handle(handle))

	// rdr := bufio.NewReader(os.Stdin)
	// fmt.Printf("press enter to continue...")
	// _, _, err = rdr.ReadLine()
	// if err != nil {
	// 	return err
	// }

	if err = computestorage.FormatWritableLayerVhd(context.TODO(), windows.Handle(handle)); err != nil {
		return err
	}
	return nil
}

func TestCreateScratchLayer(t *testing.T) {
	testDir := t.TempDir()
	vhdPath := filepath.Join(testDir, "blank-base.vhdx")
	// create a test VHD

	if err := createTestVhdx(vhdPath); err != nil {
		t.Fatalf("failed to create VHD: %s", err)
	}

	// layerRoot := "D:\\Containers\\containerplatdata\\root\\io.containerd.snapshotter.v1.windows\\snapshots\\2\\Files"
	// lWalker := &legacyLayerWalker{
	// 	root: layerRoot,
	// }
	// err := prepareScratch(context.TODO(), vhdPath, lWalker)
	// if err != nil {
	// 	t.Fatalf("failed to walk layer tree: %s", err)
	// }

}
