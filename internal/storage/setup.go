package storage

import (
	"context"
	"fmt"

	"github.com/Microsoft/hcsshim/internal/oc"
	"go.opencensus.io/trace"
	"golang.org/x/sys/windows"
)

// SetupBaseOSLayer Sets up a layer that contains a base OS for a container.
func SetupBaseOSLayer(ctx context.Context, layerPath string, vhdHandle windows.Handle, options string) (err error) {
	title := "hcsshim::SetupBaseOSLayer"
	ctx, span := trace.StartSpan(ctx, title)
	defer span.End()
	defer func() { oc.SetSpanStatus(span, err) }()
	span.AddAttributes(
		trace.StringAttribute("layerPath", layerPath),
		trace.StringAttribute("options", options),
	)

	err = hcsSetupBaseOSLayer(layerPath, vhdHandle, options)
	if err != nil {
		return fmt.Errorf("failed to setup base OS layer: %s", err)
	}
	return nil
}
