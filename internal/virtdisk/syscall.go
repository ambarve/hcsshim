package virtdisk

import (
	"context"
	"fmt"

	"github.com/Microsoft/hcsshim/internal/oc"
	"go.opencensus.io/trace"
	"golang.org/x/sys/windows"
)

func OpenVirtualDisk(ctx context.Context, vhdPath string, virtualDiskAccessMask VirtualDiskAccessMask, openVirtualDiskFlags OpenVirtualDiskFlag, parameters *OpenVirtualDiskParameters) (windows.Handle, error) {
	var handle windows.Handle
	var err error
	title := "hcsshim::OpenVirtualDisk"
	ctx, span := trace.StartSpan(ctx, title)
	defer span.End()
	defer func() { oc.SetSpanStatus(span, err) }()

	if parameters.Version != 2 {
		return handle, fmt.Errorf("only version 2 VHDs are supported, found version: %d", parameters.Version)
	}

	err = openVirtualDisk(&vhdxVirtualStorageType, vhdPath, uint32(virtualDiskAccessMask), uint32(openVirtualDiskFlags), parameters, &handle)
	if err != nil {
		return handle, fmt.Errorf("failed to open virtual disk: %s", err)
	}
	return handle, nil
}

func CreateVirtualDisk(ctx context.Context, path string, virtualDiskAccessMask VirtualDiskAccessMask, createVirtualDiskFlags CreateVirtualDiskFlag, parameters *CreateVirtualDiskParameters) (windows.Handle, error) {
	var handle windows.Handle
	var err error
	title := "hcsshim::CreateVirtualDisk"
	ctx, span := trace.StartSpan(ctx, title)
	defer span.End()
	defer func() { oc.SetSpanStatus(span, err) }()

	if parameters.Version != 2 {
		return handle, fmt.Errorf("only version 2 VHDs are supported, found version: %d", parameters.Version)
	}

	err = createVirtualDisk(&vhdxVirtualStorageType, path, uint32(virtualDiskAccessMask), 0, uint32(createVirtualDiskFlags), 0, parameters, nil, &handle)
	if err != nil {
		return handle, fmt.Errorf("failed to create virtual disk: %s", err)
	}
	return handle, nil
}

func GetVirtualDiskPhysicalPath(ctx context.Context, handle windows.Handle) (string, error) {
	var diskPathSizeInBytes uint32 = 256 * 2 // max path length 256 wide chars
	var diskPhysicalPathBuf [256]uint16
	var err error
	title := "hcsshim::GetVirtualDiskPhysicalPath"
	ctx, span := trace.StartSpan(ctx, title)
	defer span.End()
	defer func() { oc.SetSpanStatus(span, err) }()

	err = getVirtualDiskPhysicalPath(handle, &diskPathSizeInBytes, &diskPhysicalPathBuf[0])
	if err != nil {
		return "", fmt.Errorf("failed to get disk physical path: %s", err)
	}
	// convert path to UTF-8 string
	return windows.UTF16ToString(diskPhysicalPathBuf[:]), nil
}

func AttachVirtualDisk(ctx context.Context, handle windows.Handle, attachVirtualDiskFlag AttachVirtualDiskFlag, parameters *AttachVirtualDiskParameters) (err error) {
	title := "hcsshim::AttachVirtualDisk"
	ctx, span := trace.StartSpan(ctx, title)
	defer span.End()
	defer func() { oc.SetSpanStatus(span, err) }()

	if parameters.Version != 2 {
		return fmt.Errorf("only version 2 VHDs are supported, found version: %d", parameters.Version)
	}

	err = attachVirtualDisk(handle, 0, uint32(attachVirtualDiskFlag), 0, parameters, nil)
	if err != nil {
		return fmt.Errorf("failed to attach virtual disk: %s", err)
	}
	return nil
}

func DetachVirtualDisk(ctx context.Context, handle windows.Handle) (err error) {
	title := "hcsshim::DetachVirtualDisk"
	ctx, span := trace.StartSpan(ctx, title)
	defer span.End()
	defer func() { oc.SetSpanStatus(span, err) }()

	err = detachVirtualDisk(handle, 0, 0)
	if err != nil {
		return fmt.Errorf("failed to detach virtual disk: %s", err)
	}
	return nil
}
