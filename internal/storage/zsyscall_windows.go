// Code generated mksyscall_windows.exe DO NOT EDIT

package storage

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
	modcomputestorage = windows.NewLazySystemDLL("computestorage.dll")

	procHcsImportLayer                   = modcomputestorage.NewProc("HcsImportLayer")
	procHcsExportLayer                   = modcomputestorage.NewProc("HcsExportLayer")
	procHcsExportLegacyWritableLayer     = modcomputestorage.NewProc("HcsExportLegacyWritableLayer")
	procHcsDestoryLayer                  = modcomputestorage.NewProc("HcsDestoryLayer")
	procHcsSetupBaseOSLayer              = modcomputestorage.NewProc("HcsSetupBaseOSLayer")
	procHcsInitializeWritableLayer       = modcomputestorage.NewProc("HcsInitializeWritableLayer")
	procHcsInitializeLegacyWritableLayer = modcomputestorage.NewProc("HcsInitializeLegacyWritableLayer")
	procHcsAttachLayerStorageFilter      = modcomputestorage.NewProc("HcsAttachLayerStorageFilter")
	procHcsDetachLayerStorageFilter      = modcomputestorage.NewProc("HcsDetachLayerStorageFilter")
	procHcsFormatWritableLayerVhd        = modcomputestorage.NewProc("HcsFormatWritableLayerVhd")
	procHcsGetLayerVhdMountPath          = modcomputestorage.NewProc("HcsGetLayerVhdMountPath")
)

func hcsImportLayer(layerPath string, sourceFolderPath string, layerData string) (hr error) {
	var _p0 *uint16
	_p0, hr = syscall.UTF16PtrFromString(layerPath)
	if hr != nil {
		return
	}
	var _p1 *uint16
	_p1, hr = syscall.UTF16PtrFromString(sourceFolderPath)
	if hr != nil {
		return
	}
	var _p2 *uint16
	_p2, hr = syscall.UTF16PtrFromString(layerData)
	if hr != nil {
		return
	}
	return _hcsImportLayer(_p0, _p1, _p2)
}

func _hcsImportLayer(layerPath *uint16, sourceFolderPath *uint16, layerData *uint16) (hr error) {
	r0, _, _ := syscall.Syscall(procHcsImportLayer.Addr(), 3, uintptr(unsafe.Pointer(layerPath)), uintptr(unsafe.Pointer(sourceFolderPath)), uintptr(unsafe.Pointer(layerData)))
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func hcsExportLayer(layerPath string, exportFolderPath string, layerData string, options string) (hr error) {
	var _p0 *uint16
	_p0, hr = syscall.UTF16PtrFromString(layerPath)
	if hr != nil {
		return
	}
	var _p1 *uint16
	_p1, hr = syscall.UTF16PtrFromString(exportFolderPath)
	if hr != nil {
		return
	}
	var _p2 *uint16
	_p2, hr = syscall.UTF16PtrFromString(layerData)
	if hr != nil {
		return
	}
	var _p3 *uint16
	_p3, hr = syscall.UTF16PtrFromString(options)
	if hr != nil {
		return
	}
	return _hcsExportLayer(_p0, _p1, _p2, _p3)
}

func _hcsExportLayer(layerPath *uint16, exportFolderPath *uint16, layerData *uint16, options *uint16) (hr error) {
	r0, _, _ := syscall.Syscall6(procHcsExportLayer.Addr(), 4, uintptr(unsafe.Pointer(layerPath)), uintptr(unsafe.Pointer(exportFolderPath)), uintptr(unsafe.Pointer(layerData)), uintptr(unsafe.Pointer(options)), 0, 0)
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func hcsExportLegacyWritableLayer(writableLayerMountPath string, writeableLayerFolderPath string, exportFolderPath string, layerData string) (hr error) {
	var _p0 *uint16
	_p0, hr = syscall.UTF16PtrFromString(writableLayerMountPath)
	if hr != nil {
		return
	}
	var _p1 *uint16
	_p1, hr = syscall.UTF16PtrFromString(writeableLayerFolderPath)
	if hr != nil {
		return
	}
	var _p2 *uint16
	_p2, hr = syscall.UTF16PtrFromString(exportFolderPath)
	if hr != nil {
		return
	}
	var _p3 *uint16
	_p3, hr = syscall.UTF16PtrFromString(layerData)
	if hr != nil {
		return
	}
	return _hcsExportLegacyWritableLayer(_p0, _p1, _p2, _p3)
}

func _hcsExportLegacyWritableLayer(writableLayerMountPath *uint16, writeableLayerFolderPath *uint16, exportFolderPath *uint16, layerData *uint16) (hr error) {
	r0, _, _ := syscall.Syscall6(procHcsExportLegacyWritableLayer.Addr(), 4, uintptr(unsafe.Pointer(writableLayerMountPath)), uintptr(unsafe.Pointer(writeableLayerFolderPath)), uintptr(unsafe.Pointer(exportFolderPath)), uintptr(unsafe.Pointer(layerData)), 0, 0)
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func hcsDestroyLayer(layerPath string) (hr error) {
	var _p0 *uint16
	_p0, hr = syscall.UTF16PtrFromString(layerPath)
	if hr != nil {
		return
	}
	return _hcsDestroyLayer(_p0)
}

func _hcsDestroyLayer(layerPath *uint16) (hr error) {
	r0, _, _ := syscall.Syscall(procHcsDestoryLayer.Addr(), 1, uintptr(unsafe.Pointer(layerPath)), 0, 0)
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func hcsSetupBaseOSLayer(layerPath string, handle windows.Handle, options string) (hr error) {
	var _p0 *uint16
	_p0, hr = syscall.UTF16PtrFromString(layerPath)
	if hr != nil {
		return
	}
	var _p1 *uint16
	_p1, hr = syscall.UTF16PtrFromString(options)
	if hr != nil {
		return
	}
	return _hcsSetupBaseOSLayer(_p0, handle, _p1)
}

func _hcsSetupBaseOSLayer(layerPath *uint16, handle windows.Handle, options *uint16) (hr error) {
	r0, _, _ := syscall.Syscall(procHcsSetupBaseOSLayer.Addr(), 3, uintptr(unsafe.Pointer(layerPath)), uintptr(handle), uintptr(unsafe.Pointer(options)))
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func hcsInitializeWritableLayer(writableLayerPath string, layerData string, options string) (hr error) {
	var _p0 *uint16
	_p0, hr = syscall.UTF16PtrFromString(writableLayerPath)
	if hr != nil {
		return
	}
	var _p1 *uint16
	_p1, hr = syscall.UTF16PtrFromString(layerData)
	if hr != nil {
		return
	}
	var _p2 *uint16
	_p2, hr = syscall.UTF16PtrFromString(options)
	if hr != nil {
		return
	}
	return _hcsInitializeWritableLayer(_p0, _p1, _p2)
}

func _hcsInitializeWritableLayer(writableLayerPath *uint16, layerData *uint16, options *uint16) (hr error) {
	r0, _, _ := syscall.Syscall(procHcsInitializeWritableLayer.Addr(), 3, uintptr(unsafe.Pointer(writableLayerPath)), uintptr(unsafe.Pointer(layerData)), uintptr(unsafe.Pointer(options)))
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func hcsInitializeLegacyWritableLayer(mountPath string, folderPath string, layerData string, options string) (hr error) {
	var _p0 *uint16
	_p0, hr = syscall.UTF16PtrFromString(mountPath)
	if hr != nil {
		return
	}
	var _p1 *uint16
	_p1, hr = syscall.UTF16PtrFromString(folderPath)
	if hr != nil {
		return
	}
	var _p2 *uint16
	_p2, hr = syscall.UTF16PtrFromString(layerData)
	if hr != nil {
		return
	}
	var _p3 *uint16
	_p3, hr = syscall.UTF16PtrFromString(options)
	if hr != nil {
		return
	}
	return _hcsInitializeLegacyWritableLayer(_p0, _p1, _p2, _p3)
}

func _hcsInitializeLegacyWritableLayer(mountPath *uint16, folderPath *uint16, layerData *uint16, options *uint16) (hr error) {
	r0, _, _ := syscall.Syscall6(procHcsInitializeLegacyWritableLayer.Addr(), 4, uintptr(unsafe.Pointer(mountPath)), uintptr(unsafe.Pointer(folderPath)), uintptr(unsafe.Pointer(layerData)), uintptr(unsafe.Pointer(options)), 0, 0)
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func hcsAttachLayerStorageFilter(layerPath string, layerData string) (hr error) {
	var _p0 *uint16
	_p0, hr = syscall.UTF16PtrFromString(layerPath)
	if hr != nil {
		return
	}
	var _p1 *uint16
	_p1, hr = syscall.UTF16PtrFromString(layerData)
	if hr != nil {
		return
	}
	return _hcsAttachLayerStorageFilter(_p0, _p1)
}

func _hcsAttachLayerStorageFilter(layerPath *uint16, layerData *uint16) (hr error) {
	r0, _, _ := syscall.Syscall(procHcsAttachLayerStorageFilter.Addr(), 2, uintptr(unsafe.Pointer(layerPath)), uintptr(unsafe.Pointer(layerData)), 0)
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func hcsDetachLayerStorageFilter(layerPath string) (hr error) {
	var _p0 *uint16
	_p0, hr = syscall.UTF16PtrFromString(layerPath)
	if hr != nil {
		return
	}
	return _hcsDetachLayerStorageFilter(_p0)
}

func _hcsDetachLayerStorageFilter(layerPath *uint16) (hr error) {
	r0, _, _ := syscall.Syscall(procHcsDetachLayerStorageFilter.Addr(), 1, uintptr(unsafe.Pointer(layerPath)), 0, 0)
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func hcsFormatWritableLayerVhd(handle windows.Handle) (hr error) {
	r0, _, _ := syscall.Syscall(procHcsFormatWritableLayerVhd.Addr(), 1, uintptr(handle), 0, 0)
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}

func hcsGetLayerVhdMountPath(vhdHandle windows.Handle) (hr error) {
	r0, _, _ := syscall.Syscall(procHcsGetLayerVhdMountPath.Addr(), 1, uintptr(vhdHandle), 0, 0)
	if int32(r0) < 0 {
		if r0&0x1fff0000 == 0x00070000 {
			r0 &= 0xffff
		}
		hr = syscall.Errno(r0)
	}
	return
}
