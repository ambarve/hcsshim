package vhdx

import (
	"context"
	"testing"
)

func TestGetDriveDiskID(t *testing.T) {
	path := ".\\vhd\\sandbox.vhdx"
	info, err := GetScratchVhdPartitionInfo(context.TODO(), path)
	if err != nil {
		t.Fatalf("failed to get drive disk ID: %s", err)
	}
	t.Logf("disk ID: %s, partition ID:\n", info.DiskID, info.PartitionID)
}
