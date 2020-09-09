package vhdx

import (
	"context"
	"os"

	"github.com/Microsoft/hcsshim/internal/virtdisk"
	"golang.org/x/sys/windows"
)

// opens the VHDx at given path with default open flags and attaches it with given attachFlags.
// Returns the handle to the attached vhdx.
func AttachVhdx(ctx context.Context, path string, attachFlags virtdisk.AttachVirtualDiskFlag) (windows.Handle, error) {
	var (
		handle windows.Handle
		err    error
	)
	openParams := &virtdisk.OpenVirtualDiskParameters{Version: 2}
	handle, err = virtdisk.OpenVirtualDisk(ctx, path, virtdisk.VirtualDiskAccessFlagNone, virtdisk.OpenVirtualDiskFlagNone, openParams)
	if err != nil {
		return 0, &os.PathError{Op: "OpenVirtualDisk", Path: path, Err: err}
	}

	attachParams := &virtdisk.AttachVirtualDiskParameters{Version: 2}
	virtdisk.AttachVirtualDisk(ctx, handle, attachFlags, attachParams)
	if err != nil {
		windows.CloseHandle(handle)
		return 0, &os.PathError{Op: "AttachVirtualDisk", Path: path, Err: err}
	}
	return handle, nil
}
