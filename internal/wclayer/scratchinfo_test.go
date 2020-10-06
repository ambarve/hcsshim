package wclayer

import (
	"context"
	"testing"

	"github.com/Microsoft/hcsshim/internal/wclayer"
)

func TestGetDriveDiskID(t *testing.T) {
	path := ".\\vhd\\sandbox.vhdx"
	id, err := wclayer.GetScratchDriveDiskID(context.TODO(), path)
	if err != nil {
		t.Fatalf("failed to get drive disk ID: %s", err)
	}
	t.Logf("disk ID: %s\n", id)
}

func TestExpandVHD(t *testing.T) {
	path := ".\\vhd"
	err := wclayer.ExpandScratchSize(context.TODO(), path, 1024*1024*1024*40)
	if err != nil {
		t.Fatalf("failed expand drive: %s", err)
	}
}
