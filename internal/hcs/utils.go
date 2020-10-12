package hcs

import (
	"context"
	"io"
	"syscall"

	"github.com/Microsoft/go-winio"
	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/Microsoft/hcsshim/internal/storage"
	"github.com/Microsoft/hcsshim/internal/virtdisk"
	"github.com/pkg/errors"
	"golang.org/x/sys/windows"
)

// makeOpenFiles calls winio.MakeOpenFile for each handle in a slice but closes all the handles
// if there is an error.
func makeOpenFiles(hs []syscall.Handle) (_ []io.ReadWriteCloser, err error) {
	fs := make([]io.ReadWriteCloser, len(hs))
	for i, h := range hs {
		if h != syscall.Handle(0) {
			if err == nil {
				fs[i], err = winio.MakeOpenFile(h)
			}
			if err != nil {
				syscall.Close(h)
			}
		}
	}
	if err != nil {
		for _, f := range fs {
			if f != nil {
				f.Close()
			}
		}
		return nil, err
	}
	return fs, nil
}

// creates a VHD formatted with NTFS of size `sizeGB` at the given `vhdPath`.
func CreateNTFSVHD(ctx context.Context, vhdPath string, sizeGB uint32) (err error) {

	createParams := &virtdisk.CreateVirtualDiskParameters{
		Version: 2,
		Version2: virtdisk.CreateVersion2{
			MaximumSize:      uint64(sizeGB) * 1024 * 1024 * 1024,
			BlockSizeInBytes: 1 * 1024 * 1024,
		},
	}

	handle, err := virtdisk.CreateVirtualDisk(ctx, vhdPath, virtdisk.VirtualDiskAccessFlagNone, virtdisk.CreateVirtualDiskFlagNone, createParams)
	if err != nil {
		return errors.Wrap(err, "failed to create VHD")
	}
	defer func() {
		if err2 := windows.CloseHandle(handle); err2 != nil {
			log.G(ctx).Warnf("failed to close VHD handle : %s", err2)
		}
	}()

	if err := storage.FormatWritableLayerVhd(ctx, handle); err != nil {
		return errors.Wrap(err, "failed to format VHD")
	}

	return nil
}
