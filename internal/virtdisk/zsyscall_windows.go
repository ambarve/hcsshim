// Code generated mksyscall_windows.exe DO NOT EDIT

package virtdisk

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

var _ unsafe.Pointer

// Do the interface allocations only once for common
// Errno values.
const (
	errnoERROR_IO_PENDING = 997
)

var (
	errERROR_IO_PENDING error = syscall.Errno(errnoERROR_IO_PENDING)
)

// errnoErr returns common boxed Errno values, to prevent
// allocations at runtime.
func errnoErr(e syscall.Errno) error {
	switch e {
	case 0:
		return nil
	case errnoERROR_IO_PENDING:
		return errERROR_IO_PENDING
	}
	// TODO: add more here, after collecting data on the common
	// error values see on Windows. (perhaps when running
	// all.bat?)
	return e
}

var (
	modVirtDisk = windows.NewLazySystemDLL("VirtDisk.dll")
	modvirtDisk = windows.NewLazySystemDLL("virtDisk.dll")
	modvirtdisk = windows.NewLazySystemDLL("virtdisk.dll")

	procCreateVirtualDisk          = modVirtDisk.NewProc("CreateVirtualDisk")
	procOpenVirtualDisk            = modVirtDisk.NewProc("OpenVirtualDisk")
	procAttachVirtualDisk          = modVirtDisk.NewProc("AttachVirtualDisk")
	procDetachVirtualDisk          = modvirtDisk.NewProc("DetachVirtualDisk")
	procGetVirtualDiskPhysicalPath = modvirtdisk.NewProc("GetVirtualDiskPhysicalPath")
)

func createVirtualDisk(virtualStorageType *VirtualStorageType, path string, virtualDiskAccessMask uint32, securityDescriptor uintptr, createVirtualDiskFlags uint32, providerSpecificFlags uint32, parameters *CreateVirtualDiskParameters, overlapped *windows.Overlapped, handle *windows.Handle) (err error) {
	var _p0 *uint16
	_p0, err = syscall.UTF16PtrFromString(path)
	if err != nil {
		return
	}
	return _createVirtualDisk(virtualStorageType, _p0, virtualDiskAccessMask, securityDescriptor, createVirtualDiskFlags, providerSpecificFlags, parameters, overlapped, handle)
}

func _createVirtualDisk(virtualStorageType *VirtualStorageType, path *uint16, virtualDiskAccessMask uint32, securityDescriptor uintptr, createVirtualDiskFlags uint32, providerSpecificFlags uint32, parameters *CreateVirtualDiskParameters, overlapped *windows.Overlapped, handle *windows.Handle) (err error) {
	r1, _, e1 := syscall.Syscall9(procCreateVirtualDisk.Addr(), 9, uintptr(unsafe.Pointer(virtualStorageType)), uintptr(unsafe.Pointer(path)), uintptr(virtualDiskAccessMask), uintptr(securityDescriptor), uintptr(createVirtualDiskFlags), uintptr(providerSpecificFlags), uintptr(unsafe.Pointer(parameters)), uintptr(unsafe.Pointer(overlapped)), uintptr(unsafe.Pointer(handle)))
	if r1 != 0 {
		if e1 != 0 {
			err = errnoErr(e1)
		} else {
			err = syscall.EINVAL
		}
	}
	return
}

func openVirtualDisk(virtualStorageType *VirtualStorageType, path string, virtualDiskAccessMask uint32, openVirtualDiskFlags uint32, parameters *OpenVirtualDiskParameters, handle *windows.Handle) (err error) {
	var _p0 *uint16
	_p0, err = syscall.UTF16PtrFromString(path)
	if err != nil {
		return
	}
	return _openVirtualDisk(virtualStorageType, _p0, virtualDiskAccessMask, openVirtualDiskFlags, parameters, handle)
}

func _openVirtualDisk(virtualStorageType *VirtualStorageType, path *uint16, virtualDiskAccessMask uint32, openVirtualDiskFlags uint32, parameters *OpenVirtualDiskParameters, handle *windows.Handle) (err error) {
	r1, _, e1 := syscall.Syscall6(procOpenVirtualDisk.Addr(), 6, uintptr(unsafe.Pointer(virtualStorageType)), uintptr(unsafe.Pointer(path)), uintptr(virtualDiskAccessMask), uintptr(openVirtualDiskFlags), uintptr(unsafe.Pointer(parameters)), uintptr(unsafe.Pointer(handle)))
	if r1 != 0 {
		if e1 != 0 {
			err = errnoErr(e1)
		} else {
			err = syscall.EINVAL
		}
	}
	return
}

func attachVirtualDisk(handle windows.Handle, securityDescriptor uintptr, attachVirtualDiskFlag uint32, providerSpecificFlags uint32, parameters *AttachVirtualDiskParameters, overlapped *windows.Overlapped) (err error) {
	r1, _, e1 := syscall.Syscall6(procAttachVirtualDisk.Addr(), 6, uintptr(handle), uintptr(securityDescriptor), uintptr(attachVirtualDiskFlag), uintptr(providerSpecificFlags), uintptr(unsafe.Pointer(parameters)), uintptr(unsafe.Pointer(overlapped)))
	if r1 != 0 {
		if e1 != 0 {
			err = errnoErr(e1)
		} else {
			err = syscall.EINVAL
		}
	}
	return
}

func detachVirtualDisk(handle windows.Handle, detachVirtualDiskFlags uint32, providerSpecificFlags uint32) (err error) {
	r1, _, e1 := syscall.Syscall(procDetachVirtualDisk.Addr(), 3, uintptr(handle), uintptr(detachVirtualDiskFlags), uintptr(providerSpecificFlags))
	if r1 != 0 {
		if e1 != 0 {
			err = errnoErr(e1)
		} else {
			err = syscall.EINVAL
		}
	}
	return
}

func getVirtualDiskPhysicalPath(handle windows.Handle, diskPathSizeInBytes *uint32, buffer *uint16) (err error) {
	r1, _, e1 := syscall.Syscall(procGetVirtualDiskPhysicalPath.Addr(), 3, uintptr(handle), uintptr(unsafe.Pointer(diskPathSizeInBytes)), uintptr(unsafe.Pointer(buffer)))
	if r1 != 0 {
		if e1 != 0 {
			err = errnoErr(e1)
		} else {
			err = syscall.EINVAL
		}
	}
	return
}
