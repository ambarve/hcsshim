package wclayer

import (
	"context"
	"os"

	"github.com/Microsoft/hcsshim/internal/oc"
	"go.opencensus.io/trace"
)

// ProcessBaseLayer post-processes a base layer that has had its files extracted.
// The files should have been extracted to <path>\Files.
func ProcessBaseLayer(ctx context.Context, path string) (err error) {
	title := "hcsshim::ProcessBaseLayer"
	ctx, span := trace.StartSpan(ctx, title)
	defer span.End()
	defer func() { oc.SetSpanStatus(span, err) }()
	span.AddAttributes(trace.StringAttribute("path", path))

	err = processBaseImage(path)
	if err != nil {
		return &os.PathError{Op: title, Path: path, Err: err}
	}
	return nil
}

// ProcessImageEx post-processes a base layer. ProcessImageEx is essentially same as that of
// ProcessBaseLayer but it allows passing a different path (outputPath) in which the base vhd
// should be created.
func ProcessImageEx(ctx context.Context, path string, imageType uint32, vhdSizeGB uint64, processImageOptions uint32, outputPath string) (err error) {
	title := "hcsshim::ProcessImageEx"
	ctx, span := trace.StartSpan(ctx, title)
	defer span.End()
	defer func() { oc.SetSpanStatus(span, err) }()
	span.AddAttributes(trace.StringAttribute("path", path))
	span.AddAttributes(trace.Int64Attribute("imageType", int64(imageType)))
	span.AddAttributes(trace.Int64Attribute("vhdSizeGB", int64(vhdSizeGB)))
	span.AddAttributes(trace.Int64Attribute("processImageOptions", int64(processImageOptions)))
	span.AddAttributes(trace.StringAttribute("outputPath", outputPath))

	err = processImageEx(path, imageType, vhdSizeGB, processImageOptions, outputPath)
	if err != nil {
		return &os.SyscallError{Syscall: "ProcessImageEx", Err: err}
	}
	return nil
}

// ProcessUtilityVMImage post-processes a utility VM image that has had its files extracted.
// The files should have been extracted to <path>\Files.
func ProcessUtilityVMImage(ctx context.Context, path string) (err error) {
	title := "hcsshim::ProcessUtilityVMImage"
	ctx, span := trace.StartSpan(ctx, title)
	defer span.End()
	defer func() { oc.SetSpanStatus(span, err) }()
	span.AddAttributes(trace.StringAttribute("path", path))

	err = processUtilityImage(path)
	if err != nil {
		return &os.PathError{Op: title, Path: path, Err: err}
	}
	return nil
}
