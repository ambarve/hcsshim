package storage

import (
	"context"
	"fmt"

	"github.com/Microsoft/hcsshim/internal/oc"
	"go.opencensus.io/trace"
	"golang.org/x/sys/windows"
)

// FormatWritableLayerVhd formats a virtual disk for use as a writable container layer.
func FormatWritableLayerVhd(ctx context.Context, vhdHandle windows.Handle) (err error) {
	title := "hcsshim::FormatWritableLayerVhd"
	ctx, span := trace.StartSpan(ctx, title)
	defer span.End()
	defer func() { oc.SetSpanStatus(span, err) }()

	err = hcsFormatWritableLayerVhd(vhdHandle)
	if err != nil {
		return fmt.Errorf("failed to format writable layer vhd: %s", err)
	}
	return nil
}
