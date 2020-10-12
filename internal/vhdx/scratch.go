package vhdx

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/Microsoft/go-winio/pkg/guid"
	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/Microsoft/hcsshim/internal/oc"
	"github.com/Microsoft/hcsshim/internal/virtdisk"
	"github.com/sirupsen/logrus"
	"go.opencensus.io/trace"
	"golang.org/x/sys/windows"
)

// Scratch VHDs are formatted with GPT style and have 1 MSFT_RESERVED
// partition and 1 BASIC_DATA partition.  This struct contains the
// partitionID of this BASIC_DATA partition and the DiskID of this
// scratch vhdx.
type ScratchVhdxPartitionInfo struct {
	DiskID      guid.GUID
	PartitionID guid.GUID
}

// Returns the VhdxInfo of a GPT vhdx at path vhdxPath.
func GetScratchVhdPartitionInfo(ctx context.Context, vhdxPath string) (_ ScratchVhdxPartitionInfo, err error) {
	var (
		diskHandle       windows.Handle
		driveLayout      driveLayoutInformationEx
		partitions       []partitionInformationEx
		gptDriveLayout   driveLayoutInformationGPT
		gptPartitionInfo partitionInformationGPT
	)

	title := "hcsshim::GetScratchVhdPartitionInfo"
	ctx, span := trace.StartSpan(ctx, title)
	defer span.End()
	defer func() { oc.SetSpanStatus(span, err) }()
	span.AddAttributes(
		trace.StringAttribute("path", vhdxPath))

	diskHandle, err = AttachVhdx(ctx, vhdxPath, virtdisk.AttachVirtualDiskFlagBypassDefaultEncryptionPolicy|virtdisk.AttachVirtualDiskFlagNoDriveLetter)
	if err != nil {
		return ScratchVhdxPartitionInfo{}, fmt.Errorf("attach vhd failed: %s", err)
	}
	defer func() {
		if err2 := virtdisk.DetachVirtualDisk(ctx, diskHandle); err2 != nil {
			if err == nil {
				err = err2
			}
		}
		if err2 := windows.CloseHandle(diskHandle); err2 != nil {
			if err == nil {
				err = err2
			}
		}
	}()

	driveLayout, partitions, err = getDriveLayout(ctx, diskHandle)
	if err != nil {
		return ScratchVhdxPartitionInfo{}, err
	}

	if driveLayout.PartitionStyle != PARTITION_STYLE_GPT {
		return ScratchVhdxPartitionInfo{}, fmt.Errorf("drive Layout:Expected partition style GPT(%d) found %d", PARTITION_STYLE_GPT, driveLayout.PartitionStyle)
	}

	if driveLayout.PartitionCount != 2 || len(partitions) != 2 {
		return ScratchVhdxPartitionInfo{}, fmt.Errorf("expected exactly 2 partitions. Got %d partitions and partition count of %d", len(partitions), driveLayout.PartitionCount)
	}

	if partitions[1].PartitionStyle != PARTITION_STYLE_GPT {
		return ScratchVhdxPartitionInfo{}, fmt.Errorf("partition Info:Expected partition style GPT(%d) found %d", PARTITION_STYLE_GPT, partitions[1].PartitionStyle)
	}

	bufReader := bytes.NewBuffer(driveLayout.GptMbrUnion[:])
	if err := binary.Read(bufReader, binary.LittleEndian, &gptDriveLayout); err != nil {
		return ScratchVhdxPartitionInfo{}, fmt.Errorf("failed to parse drive GPT layout: %s", err)
	}

	bufReader = bytes.NewBuffer(partitions[1].GptMbrUnion[:])
	if err := binary.Read(bufReader, binary.LittleEndian, &gptPartitionInfo); err != nil {
		return ScratchVhdxPartitionInfo{}, fmt.Errorf("failed to parse GPT partition info: %s", err)
	}

	if gptPartitionInfo.PartitionType != PARTITION_BASIC_DATA_GUID {
		return ScratchVhdxPartitionInfo{}, fmt.Errorf("expected partition type to have %s GUID found %s instead", PARTITION_BASIC_DATA_GUID, gptPartitionInfo.PartitionType)
	}

	log.G(ctx).WithFields(logrus.Fields{
		"Disk ID":          gptDriveLayout.DiskID,
		"GPT Partition ID": gptPartitionInfo.PartitionId,
	}).Debug("Scratch VHD partition info")

	return ScratchVhdxPartitionInfo{DiskID: gptDriveLayout.DiskID, PartitionID: gptPartitionInfo.PartitionId}, nil

}
