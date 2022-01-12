package wclayer

import (
	"bytes"
	"context"
	"encoding/binary"
	"path/filepath"
	"unicode/utf16"

	winio "github.com/Microsoft/go-winio"
	"github.com/Microsoft/go-winio/pkg/guid"
	"github.com/pkg/errors"
	"golang.org/x/sys/windows"
)

const (
	WCIFSCurrentVersion uint32 = 1
	wcifsReparseTag     uint32 = 0x90001018
)

var (
	utilityVMLayerID = guid.GUID{
		Data1: 0x1b3979c8,
		Data2: 0x279b,
		Data3: 0x42eb,
		Data4: [8]byte{0xb2, 0xb9, 0x75, 0x07, 0x67, 0xee, 0x9e, 0x3f},
	}
)

// wcReparseInfo represents the WCIFs specific information that will be set
// in the reparse point.
type wcReparseInfo struct {
	version uint32
	flags   uint32
	layerID guid.GUID
	// relative path of the file that this reparse point represents.
	name string
}

// encode encodes the wcReparseInfo buffer into a byte array so that it can be passed
// to reparse point creation functions.
func (wc *wcReparseInfo) encode() []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, wc.version)
	binary.Write(&buf, binary.LittleEndian, wc.flags)
	binary.Write(&buf, binary.LittleEndian, wc.layerID)
	guidArray := utilityVMLayerID.ToWindowsArray()
	buf.Write(guidArray[:])

	// convert the name string to UTF16 without using the terminating null char
	utf16name := utf16.Encode([]rune(wc.name))
	// size of array in bytes - uint16
	binary.Write(&buf, binary.LittleEndian, uint16(len(utf16name)*2))
	binary.Write(&buf, binary.LittleEndian, utf16name)
	return buf.Bytes()
}

type wcifsReparsePointCreator struct {
	// The target under which wcifs reparse points should be created.
	// This usually is the path to the volume at which scratch VHD is mounted.
	targetPath string
}

// createWciReparsePoint creates a WCIFS reparse points at path `wc.targetPath + path`.
func (wc *wcifsReparsePointCreator) createWciReparsePoint(ctx context.Context, path string, fiProvider LayerFileInfoProvider) error {
	// exclude these files.
	if path == "." || path == ".." || path == "$Recycle.Bin" {
		return nil

	}
	destPath := filepath.Join(wc.targetPath, path)
	utf16DstPath := utf16.Encode([]rune(destPath))

	srcBasicInfo, err := fiProvider.GetFileBasicInformation(path)
	if err != nil {
		return errors.Wrapf(err, "failed to get basic info for %s", path)
	}

	srcStdInfo, err := fiProvider.GetFileStandardInformation(path)
	if err != nil {
		return errors.Wrapf(err, "failed to get standard info for %s", path)
	}

	if srcBasicInfo.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT == 0 {
		// create a new reparse point
		reparseInfo := wcReparseInfo{
			version: WCIFSCurrentVersion,
			layerID: utilityVMLayerID,
			name:    destPath,
		}

		encodedInfo := reparseInfo.encode()
		reparseBuf := winio.ReparseDataBuffer{
			ReparseTag:        wcifsReparseTag,
			ReparseDataLength: uint16(len(encodedInfo)),
			DataBuffer:        encodedInfo,
		}
		if err = winio.SetReparsePoint(destPath, reparseBuf.Encode()); err != nil {
			return errors.Wrapf(err, "set reparse point failed for file: %s", path)
		}

		reparseHandle, err := windows.CreateFile(&utf16DstPath[0], (windows.GENERIC_READ | windows.GENERIC_WRITE), 0, nil, windows.OPEN_EXISTING, (windows.FILE_ATTRIBUTE_NORMAL | windows.FILE_FLAG_OPEN_REPARSE_POINT), 0)
		if err != nil {
			return errors.Errorf("failed to open reparse point with: %s", err)
		}
		defer windows.Close(reparseHandle)

		fileAttrs := (srcBasicInfo.FileAttributes &^ windows.FILE_ATTRIBUTE_ENCRYPTED) & windows.FILE_ATTRIBUTE_READONLY
		dstBasicInfo := &winio.FileBasicInfo{
			CreationTime:   srcBasicInfo.CreationTime,
			LastAccessTime: srcBasicInfo.LastAccessTime,
			LastWriteTime:  srcBasicInfo.LastWriteTime,
			ChangeTime:     srcBasicInfo.ChangeTime,
			FileAttributes: fileAttrs,
		}

		if err := winio.SetFileBasicInfoByHandle(reparseHandle, dstBasicInfo); err != nil {
			return errors.Wrapf(err, "failed to set file info for file: %s", path)
		}

		// set file size and valid length
		var lowOffset, highOffset int32
		lowOffset = int32(srcStdInfo.EndOfFile)
		highOffset = int32(srcStdInfo.EndOfFile >> 32)
		if _, err = windows.SetFilePointer(reparseHandle, lowOffset, &highOffset, windows.FILE_BEGIN); err != nil {
			return errors.Wrapf(err, "failed to set size of reparse point for %s", path)
		}
		if err = windows.SetEndOfFile(reparseHandle); err != nil {
			return errors.Wrapf(err, "failed to set end of file for %s", path)
		}

		//TODO(ambarve): ideally we also want to copy alternate data streams,
		//security descriptors and file compression information of the source file
		//to the reparse point. However, at least as of now, there is no way of
		//specifying those things when writing a container layer. So it is okay
		//even if we ignore that here.
	} else {
		// copy as it is
	}
	return nil
}
