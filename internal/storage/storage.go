// Package storage is a wrapper around the HCS storage APIs. These are new storage APIs introduced
// separate from the original graphdriver calls intended to give more freedom around creating
// and managing container layers and scratch spaces.
package storage

//go:generate go run ../../mksyscall_windows.go -output zsyscall_windows.go storage.go

//sys hcsImportLayer(layerPath string, sourceFolderPath string, layerData string) (hr error) = computestorage.HcsImportLayer
//sys hcsExportLayer(layerPath string, exportFolderPath string, layerData string, options string) (hr error) = computestorage.HcsExportLayer
//sys hcsExportLegacyWritableLayer(writableLayerMountPath string, writeableLayerFolderPath string, exportFolderPath string, layerData string) (hr error) = computestorage.HcsExportLegacyWritableLayer
//sys hcsDestroyLayer(layerPath string) (hr error) = computestorage.HcsDestoryLayer
//sys hcsSetupBaseOSLayer(layerPath string, handle windows.Handle, options string) (hr error) = computestorage.HcsSetupBaseOSLayer
//sys hcsInitializeWritableLayer(writableLayerPath string, layerData string, options string) (hr error) = computestorage.HcsInitializeWritableLayer
//sys hcsInitializeLegacyWritableLayer(mountPath string, folderPath string, layerData string, options string) (hr error) = computestorage.HcsInitializeLegacyWritableLayer
//sys hcsAttachLayerStorageFilter(layerPath string, layerData string) (hr error) = computestorage.HcsAttachLayerStorageFilter
//sys hcsDetachLayerStorageFilter(layerPath string) (hr error) = computestorage.HcsDetachLayerStorageFilter
//sys hcsFormatWritableLayerVhd(handle windows.Handle) (hr error) = computestorage.HcsFormatWritableLayerVhd
//sys hcsGetLayerVhdMountPath(vhdHandle windows.Handle) (hr error) = computestorage.HcsGetLayerVhdMountPath
