package main

import (
	"fmt"
	"path/filepath"

	"github.com/Microsoft/hcsshim/internal/appargs"
	"github.com/Microsoft/hcsshim/internal/cimfs"
	cimlayer "github.com/Microsoft/hcsshim/internal/wclayer/cim"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
)

var cimMountCommand = cli.Command{
	Name:      "cim-mount",
	Usage:     "mounts a cim to a volume, optionally mounts that volume to a provided target",
	ArgsUsage: "<cim layer path> [target path]",
	Before:    appargs.Validate(appargs.NonEmptyString, appargs.Optional(appargs.String)),
	Action: func(context *cli.Context) (err error) {
		path, err := filepath.Abs(context.Args().Get(0))
		if err != nil {
			return err
		}

		targetPath, err := filepath.Abs(context.Args().Get(1))
		if err != nil {
			return err
		}

		cimPath := cimlayer.GetCimPathFromLayer(path)
		mountPath, err := cimfs.Mount(cimPath)
		if err != nil {
			return errors.Wrap(err, "failed to mount the cim")
		}

		if context.NArg() == 2 {
			if err = setVolumeMountPoint(targetPath, mountPath); err != nil {
				return err
			}
			_, err = fmt.Println(targetPath)
			return err
		}

		_, err = fmt.Println(mountPath)
		return err
	},
}

var cimUnmountCommand = cli.Command{
	Name:      "cim-unmount",
	Usage:     "unmounts the cim mounted at volume <volume path> , optionally unmounting the target at which the volume is mounted",
	UsageText: "<volume path> should be the path in volume format '\\\\?\\Volume{GUID}'",
	ArgsUsage: "<volume path> [target path]",
	Before:    appargs.Validate(appargs.NonEmptyString, appargs.Optional(appargs.String)),
	Action: func(context *cli.Context) (err error) {
		path, err := filepath.Abs(context.Args().Get(0))
		if err != nil {
			return err
		}

		mountedPath, err := filepath.Abs(context.Args().Get(1))
		if err != nil {
			return err
		}

		if context.NArg() == 2 {
			if err = deleteVolumeMountPoint(mountedPath); err != nil {
				return err
			}
		}

		return cimfs.Unmount(path)
	},
}
