/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package mount

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Microsoft/hcsshim"
)

var (
	// ErrNotImplementOnWindows is returned when an action is not implemented for windows
	ErrNotImplementOnWindows = errors.New("not implemented under windows")
	hostMounts               = make(map[string]*Mount)
)

func (m *Mount) Mount(target string) (err error) {
	if _, ok := hostMounts[target]; ok {
		return fmt.Errorf("another layer is already mounted at %s", target)
	}
	if m.Type == "cimfs" {
		_, err = cimMount(m, target)
	} else {
		err = legacyMount(m, target)
	}
	if err == nil {
		hostMounts[target] = m
	}
	return err
}

// Mount to the provided target
func legacyMount(m *Mount, target string) error {
	if m.Type != "windows-layer" {
		return fmt.Errorf("invalid windows mount type: '%s'", m.Type)
	}

	home, layerID := filepath.Split(m.Source)

	parentLayerPaths, err := m.GetParentPaths()
	if err != nil {
		return err
	}

	var di = hcsshim.DriverInfo{
		HomeDir: home,
	}

	if err = hcsshim.ActivateLayer(di, layerID); err != nil {
		return fmt.Errorf("failed to activate layer %s: %w", m.Source, err)
	}

	if err = hcsshim.PrepareLayer(di, layerID, parentLayerPaths); err != nil {
		return fmt.Errorf("failed to prepare layer %s: %w", m.Source, err)
	}

	// We can link the layer mount path to the given target. It is an UNC path, and it needs
	// a trailing backslash.
	mountPath, err := hcsshim.GetLayerMountPath(di, layerID)
	if err != nil {
		return fmt.Errorf("failed to get layer mount path for %s: %w", m.Source, err)
	}
	mountPath = mountPath + `\`
	if err = os.Symlink(mountPath, target); err != nil {
		return fmt.Errorf("failed to link mount to taget %s: %w", target, err)
	}
	return nil
}

// ParentLayerPathsFlag is the options flag used to represent the JSON encoded
// list of parent layers required to use the layer
const ParentLayerPathsFlag = "parentLayerPaths="

// GetParentPaths of the mount
func (m *Mount) GetParentPaths() ([]string, error) {
	var parentLayerPaths []string
	for _, option := range m.Options {
		if strings.HasPrefix(option, ParentLayerPathsFlag) {
			err := json.Unmarshal([]byte(option[len(ParentLayerPathsFlag):]), &parentLayerPaths)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal parent layer paths from mount: %w", err)
			}
		}
	}
	return parentLayerPaths, nil
}

// Unmount the mount at the provided path
func Unmount(mount string, flags int) error {
	if _, ok := hostMounts[mount]; !ok {
		return nil
	}
	// unmount procedure is same for both cimfs & legacy in this case.
	return legacyUnmount(mount, flags)
}

// UnmountAll unmounts from the provided path
func UnmountAll(mount string, flags int) error {
	return Unmount(mount, flags)
}

// Unmount the mount at the provided path
func legacyUnmount(mount string, flags int) error {
	var (
		home, layerID = filepath.Split(mount)
		di            = hcsshim.DriverInfo{
			HomeDir: home,
		}
	)

	if err := hcsshim.UnprepareLayer(di, layerID); err != nil {
		return fmt.Errorf("failed to unprepare layer %s: %w", mount, err)
	}
	if err := hcsshim.DeactivateLayer(di, layerID); err != nil {
		return fmt.Errorf("failed to deactivate layer %s: %w", mount, err)
	}

	return nil
}
