//go:build windows
// +build windows

package cimfs

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"unsafe"

	hcsschema "github.com/Microsoft/hcsshim/internal/hcs/schema2"
	"github.com/Microsoft/hcsshim/internal/winapi"
)

// CIMStatFile stats a file inside the given CIM without actually mounting the CIM
func CIMStatFile(ctx context.Context, filePath string, cim *BlockCIM) (_ *winapi.FileStatBasicInformation, err error) {
	if cim.Type != BlockCIMTypeSingleFile {
		return nil, fmt.Errorf("CIM state file only works for single file CIM")
	}

	var statData winapi.FileStatBasicInformation
	err = winapi.CimGetFileStatBasicInformation2(filepath.Join(cim.BlockPath, cim.CimName), filePath, &statData, hcsschema.CimStatFileFlagSingleFileCIM)
	if err != nil {
		return nil, fmt.Errorf("failed to stat file inside the CIM: %w", err)
	}
	return &statData, nil

}

type cimFileReader struct {
	ctx            context.Context
	cimPath        string
	filePath       string
	currOffset     uint64
	bytesRemaining uint64
}

func (r *cimFileReader) Read(p []byte) (n int, err error) {
	if r.bytesRemaining == 0 {
		return 0, io.EOF
	}
	var bytesRead uint64
	err = winapi.CimReadFile2(r.cimPath, r.filePath, r.currOffset, unsafe.Pointer(&p[0]), uint64(len(p)), &bytesRead, &r.bytesRemaining, hcsschema.CimReadFileFlagSingleFileCIM)
	r.currOffset += bytesRead
	return int(bytesRead), err
}

// GetCIMFileReader creates a reader for a file at `path` inside the given CIM.  Note that
// this reader reads the file from the CIM without mounting the CIM. If the file doesn't
// exist that error will be returned in the first read call.
// Also, note that this only works for single file CIMs
func GetCIMFileReader(ctx context.Context, filePath string, cim *BlockCIM) (_ io.Reader, err error) {
	if _, err = CIMStatFile(ctx, filePath, cim); err != nil {
		return nil, err
	}

	return &cimFileReader{
		ctx:        ctx,
		cimPath:    filepath.Join(cim.BlockPath, cim.CimName),
		filePath:   filePath,
		currOffset: 0,
		// setting this to non zero value will ensure first Read call doesn't
		// return io.EOF, after that this will be set to the accurate number
		bytesRemaining: 1,
	}, nil
}
