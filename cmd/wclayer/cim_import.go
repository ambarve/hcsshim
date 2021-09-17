package main

import (
	"context"
	"os"
	"path/filepath"

	"github.com/Microsoft/go-winio"
	"github.com/Microsoft/hcsshim/internal/appargs"
	"github.com/Microsoft/hcsshim/pkg/ociwclayer"
	"github.com/urfave/cli"
)

var cimImportCommand = cli.Command{
	Name:        "cim-import",
	Usage:       "imports a CIM layer from a tar file",
	Description: "imports a CIM layer from a tar file. A directory named 'cim-layers' will be created next to the '<layer path>' directory (if it doesn't already exist) to hold the cim files. If there are any parent layers they must have been created alongside the layer directory of the current layer so that their cim files are stored in the same 'cim-layers' directory. For example, if a layer '2' is being imported at path '/foo/bar/2' then the cim files for that layer will be written to '/foo/bar/cim-layers'. If layer '1' is the parent of layer '2' then layer '1' should have been imported at '/foo/bar/1' so that cim files of layer '1' will automatically be stored at '/foo/bar/cim-layers'.",
	Flags: []cli.Flag{
		cli.StringSliceFlag{
			Name:  "layer, l",
			Usage: "path to the read-only parent layer. Only one parent path (i.e the immediate parent) should be specified",
		},
		cli.StringFlag{
			Name:  "input, i",
			Usage: "input layer tar (defaults to stdin)",
		},
	},
	ArgsUsage: "<layer path>",
	Before:    appargs.Validate(appargs.NonEmptyString),
	Action: func(cliContext *cli.Context) (err error) {
		path, err := filepath.Abs(cliContext.Args().First())
		if err != nil {
			return err
		}

		layers, err := normalizeLayers(cliContext.StringSlice("layer"), false)
		if err != nil {
			return err
		}

		fp := cliContext.String("input")
		f := os.Stdin
		if fp != "" {
			f, err = os.Open(fp)
			if err != nil {
				return err
			}
			defer f.Close()
		}
		r, err := addDecompressor(f)
		if err != nil {
			return err
		}
		err = winio.EnableProcessPrivileges([]string{winio.SeBackupPrivilege, winio.SeRestorePrivilege})
		if err != nil {
			return err
		}
		_, err = ociwclayer.ImportCimLayerFromTar(context.Background(), r, path, layers)
		return err
	},
}
