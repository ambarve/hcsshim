package layer

import (
	"path/filepath"
)

const (
	// name of the directory in which cims are stored
	cimDir = "cim-layers"

	// The name assigned to the vsmb share which shares the cim directory inside the uvm.
	CimVsmbShareName = "bootcimdir"
)

// Usually layers are stored at
// ./root/io.containerd.snapshotter.v1.windows/snapshots/<layerid>. For
// cimfs we must store all layers in the same directory (for forked
// cims to work). So all cim layers are stored in
// /root/io.containerd.snapshotter.v1.windows/snapshots/cim-layers. And
// the cim file representing each individual layer is stored at
// /root/io.containerd.snapshotter.v1.windows/snapshots/cim-layers/<layerid>.cim

// CimName is the filename (<layerid>.cim) of the file representing the cim
func GetCimNameFromLayer(layerPath string) string {
	return filepath.Base(layerPath) + ".cim"
}

// CimPath is the path to the CimDir/<layerid>.cim file that represents a layer cim.
func GetCimPathFromLayer(layerPath string) string {
	layerId := filepath.Base(layerPath)
	dir := filepath.Dir(layerPath)
	return filepath.Join(dir, cimDir, layerId+".cim")
}

// CimDir is the directory inside which all cims are stored.
func GetCimDirFromLayer(layerPath string) string {
	dir := filepath.Dir(layerPath)
	return filepath.Join(dir, cimDir)
}
