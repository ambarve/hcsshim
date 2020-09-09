// vhdx package adds the utility methods necessary to deal with the vhdx that are used as the scratch
// space for the containers and the uvm.
package vhdx

import (
	"context"
	"fmt"
	"os"
	"unsafe"

	"github.com/Microsoft/go-winio/pkg/guid"
	"github.com/Microsoft/hcsshim/internal/virtdisk"
	"golang.org/x/sys/windows"
)

const _IOCTL_DISK_GET_DRIVE_LAYOUT_EX = 0x00070050

var PARTITION_BASIC_DATA_GUID = guid.GUID{
	Data1: 0xebd0a0a2,
	Data2: 0xb9e5,
	Data3: 0x4433,
	Data4: [8]byte{0x87, 0xc0, 0x68, 0xb6, 0xb7, 0x26, 0x99, 0xc7},
}

const (
	PARTITION_STYLE_MBR uint32 = iota
	PARTITION_STYLE_GPT
	PARTITION_STYLE_RAW
)

type partitionInformationMBR struct {
	PartitionType       uint8
	BootIndicator       uint8
	RecognizedPartition uint8
	HiddenSectors       uint32
	PartitionId         guid.GUID
}

type partitionInformationGPT struct {
	PartitionType guid.GUID
	PartitionId   guid.GUID
	Attributes    uint64
	Name          [72]byte // wide char
}

type partitionInformationEx struct {
	PartitionStyle     uint32
	StartingOffset     int64
	PartitionLength    int64
	PartitionNumber    uint32
	RewritePartition   uint8
	IsServicePartition uint8
	_                  uint16
	// A union of partitionInformationMBR and partitionInformationGPT
	// since partitionInformationGPT is largest with 112 bytes
	GptMbrUnion [112]byte
}

type driveLayoutInformationGPT struct {
	DiskID               guid.GUID
	StartingUsableOffset int64
	UsableLength         int64
	MaxPartitionCount    uint32
}

type driveLayoutInformationMBR struct {
	Signature uint32
	Checksum  uint32
}

type driveLayoutInformationEx struct {
	PartitionStyle uint32
	PartitionCount uint32
	// A union of driveLayoutInformationGPT and driveLayoutInformationMBR
	// since driveLayoutInformationGPT is largest with 40 bytes
	GptMbrUnion    [40]byte
	PartitionEntry [1]partitionInformationEx
}

// Takes the handle to an attached vhdx and retrieves the drive layout information of that vhdx.
// Returns the driveLayoutInformationEx struct and a slice of partitionInfomrationEx struct containing
// one element for each partition found on the vhdx. Note: some of the members like (GptMbrUnion) of these
// structs are raw byte array and it is the responsibility of the calling function to properly parse them.
func getDriveLayout(ctx context.Context, diskHandle windows.Handle) (driveLayoutInformationEx, []partitionInformationEx, error) {
	var (
		outBytes   uint32
		err        error
		volume     *os.File
		volumePath string
	)

	layoutData := struct {
		info driveLayoutInformationEx
		// driveLayoutInformationEx has a flexible array member at the end. The data returned
		// by IOCTL_DISK_GET_DRIVE_LAYOUT_EX usually has driveLayoutInformationEx.PartitionCount
		// number of elements in this array. For all practical purposes we don't expect to have
		// more than 64 partitions in a container/uvm vhdx.
		partitions [63]partitionInformationEx
	}{}

	volumePath, err = virtdisk.GetVirtualDiskPhysicalPath(ctx, diskHandle)
	if err != nil {
		return layoutData.info, layoutData.partitions[:0], err
	}

	volume, err = os.OpenFile(volumePath, os.O_RDONLY, 0)
	if err != nil {
		return layoutData.info, layoutData.partitions[:0], fmt.Errorf("failed to open drive: %s", err)
	}
	defer volume.Close()

	err = windows.DeviceIoControl(windows.Handle(volume.Fd()),
		_IOCTL_DISK_GET_DRIVE_LAYOUT_EX,
		nil,
		0,
		(*byte)(unsafe.Pointer(&layoutData)),
		uint32(unsafe.Sizeof(layoutData)),
		&outBytes,
		nil)
	if err != nil {
		return layoutData.info, layoutData.partitions[:0], fmt.Errorf("IOCTL to get disk layout failed: %s", err)
	}

	if layoutData.info.PartitionCount == 0 {
		return layoutData.info, []partitionInformationEx{}, nil
	} else {
		// parse the retrieved data into driveLayoutInformationEx and partitionInformationEx
		partitions := make([]partitionInformationEx, layoutData.info.PartitionCount)
		partitions[0] = layoutData.info.PartitionEntry[0]
		copy(partitions[1:], layoutData.partitions[:layoutData.info.PartitionCount-1])
		return layoutData.info, partitions, nil
	}
}
