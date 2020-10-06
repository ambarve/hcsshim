package wclayer

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
	"unicode/utf16"
	"unsafe"

	"github.com/Microsoft/go-winio/pkg/guid"
	"github.com/Microsoft/hcsshim/internal/mylogger"
	"github.com/Microsoft/hcsshim/internal/oc"
	"go.opencensus.io/trace"
)

const _IOCTL_DISK_GET_DRIVE_LAYOUT_EX = 0x00070050

// GetScratchDriveDiskID retrieves the disk ID of the given vhd
func GetScratchDriveDiskIDPartitionID(ctx context.Context, path string) (string, string, error) {
	var err error
	title := "hcsshim::GetScratchDriveLayout"
	ctx, span := trace.StartSpan(ctx, title)
	defer span.End()
	defer func() { oc.SetSpanStatus(span, err) }()
	span.AddAttributes(
		trace.StringAttribute("path", path))

	mylogger.LogFmt("calling getDRiveLAyout now..\n")
	diskID, partitionID, err := getDriveLayout(ctx, path)
	if err != nil {
		return "", "", err
	}

	return diskID, partitionID, nil
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

func getDriveLayout(ctx context.Context, path string) (string, string, error) {
	var (
		diskHandle           syscall.Handle
		outBytes             uint32
		err                  error
		volume               *os.File
		volumePath           string
		gptDriveLayout       driveLayoutInformationGPT
		gptParitionInfo      partitionInformationGPT
		diskPhysicalPathSize uint32
		diskPhysicalPathBuf  [256]uint16 // max path length 256 wide char
	)

	mylogger.LogFmt("attaching vhd %s\n", path)

	diskHandle, err = attachVhd(path, ATTACH_VIRTUAL_DISK_FLAG_BYPASS_DEFAULT_ENCRYPTION_POLICY|ATTACH_VIRTUAL_DISK_FLAG_NO_DRIVE_LETTER)
	if err != nil {
		return "", "", fmt.Errorf("attach vhd failed: %s", err)
	}
	defer syscall.Close(diskHandle)

	mylogger.LogFmt("attach vhd done..\n")

	diskPhysicalPathSize = 256 * 2
	if err := getVirtualDiskPhysicalPath(diskHandle, &diskPhysicalPathSize, &diskPhysicalPathBuf[0]); err != nil {
		return "", "", fmt.Errorf("failed to get physical path of disk: %s", err)
	}

	volumePath = string(utf16.Decode(diskPhysicalPathBuf[:(diskPhysicalPathSize/2)-1]))
	mylogger.LogFmt("mount path: %s\n", volumePath)

	volume, err = os.OpenFile(volumePath, os.O_RDONLY, 0)
	if err != nil {
		return "", "", fmt.Errorf("failed to open drive: %s", err)
	}
	defer volume.Close()

	mylogger.LogFmt("volume opened\n")

	layoutData := struct {
		info driveLayoutInformationEx
		// Original struct has a 1 element array at the end. The disk that we are
		// doing IOCTL for has 2 GPT partitions. Add element for another partition
		// and use this anonymous struct for IOCTL.
		partitions [1]partitionInformationEx
	}{}

	err = syscall.DeviceIoControl(
		syscall.Handle(volume.Fd()),
		_IOCTL_DISK_GET_DRIVE_LAYOUT_EX,
		nil,
		0,
		(*byte)(unsafe.Pointer(&layoutData)),
		uint32(unsafe.Sizeof(layoutData)),
		&outBytes,
		nil)
	if err != nil {
		return "", "", fmt.Errorf("IOCTL to get disk layout failed: %s", err)
	}

	if outBytes != uint32(unsafe.Sizeof(layoutData)) {
		fmt.Errorf("ioctl data read failure. Read %d bytes, expected: %d", outBytes, unsafe.Sizeof(layoutData))
	}
	mylogger.LogFmt("ioctl done, part(%d) start: %d, length: %d, style:%d\n part(%d) start: %d, length: %d, style: %d\n",
		layoutData.info.PartitionEntry[0].PartitionNumber,
		layoutData.info.PartitionEntry[0].StartingOffset,
		layoutData.info.PartitionEntry[0].PartitionLength,
		layoutData.info.PartitionEntry[0].PartitionStyle,
		layoutData.partitions[0].PartitionNumber,
		layoutData.partitions[0].StartingOffset,
		layoutData.partitions[0].PartitionLength,
		layoutData.partitions[0].PartitionStyle)

	bufReader := bytes.NewBuffer(layoutData.partitions[0].GptMbrUnion[:])
	if err := binary.Read(bufReader, binary.LittleEndian, &gptParitionInfo); err != nil {
		return "", "", fmt.Errorf("failed to parse GPT partition info: %s", err)
	}
	mylogger.LogFmt("partition ID:%s\n", gptParitionInfo.PartitionId)

	bufReader = bytes.NewBuffer(layoutData.info.GptMbrUnion[:])
	if err := binary.Read(bufReader, binary.LittleEndian, &gptDriveLayout); err != nil {
		return "", "", fmt.Errorf(" failed to parse drive GPT layout: %s", err)
	}
	mylogger.LogFmt("DiskID: %s\n", gptDriveLayout.DiskID)

	return gptDriveLayout.DiskID.String(), gptParitionInfo.PartitionId.String(), nil
}
