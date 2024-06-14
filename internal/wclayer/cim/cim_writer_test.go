//go:build windows

package cim

import (
	"context"
	"testing"

	"github.com/Microsoft/hcsshim/pkg/cimfs"
)

func TestSingleFileWriterTypeMismatch(t *testing.T) {
	layer := &cimfs.BlockCIM{
		Type:      cimfs.BlockCIMTypeSingleFile,
		BlockPath: "",
		CimName:   "",
	}

	parent := &cimfs.BlockCIM{
		Type:      cimfs.BlockCIMTypeDevice,
		BlockPath: "",
		CimName:   "",
	}

	_, err := NewBlockCIMLayerWriter(context.TODO(), layer, []*cimfs.BlockCIM{parent})
	if err != ErrBlockCIMParentTypeMismatch {
		t.Fatalf("expected error `%s`, got `%s`", ErrBlockCIMParentTypeMismatch, err)
	}
}

func TestSingleFileWriterInvalidBlockType(t *testing.T) {
	layer := &cimfs.BlockCIM{
		BlockPath: "",
		CimName:   "",
	}

	parent := &cimfs.BlockCIM{
		BlockPath: "",
		CimName:   "",
	}

	_, err := NewBlockCIMLayerWriter(context.TODO(), layer, []*cimfs.BlockCIM{parent})
	if err != ErrBlockCIMWriterNotSupported {
		t.Fatalf("expected error `%s`, got `%s`", ErrBlockCIMWriterNotSupported, err)
	}
}
