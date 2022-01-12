package wclayer

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	winio "github.com/Microsoft/go-winio"
)

type stdFileInfoProvider struct {
	root string
}

func (t *stdFileInfoProvider) GetFileBasicInformation(path string) (*winio.FileBasicInfo, error) {
	f, err := os.Open(filepath.Join(t.root, path))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return winio.GetFileBasicInfo(f)
}

func (t *stdFileInfoProvider) GetFileStandardInformation(path string) (*winio.FileStandardInfo, error) {
	f, err := os.Open(filepath.Join(t.root, path))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return winio.GetFileStandardInfo(f)
}

func TestCreateReparsePoint(t *testing.T) {
	tempDir := t.TempDir()
	testfilePath := filepath.Join(tempDir, "test.txt")
	testfile, err := os.Create(testfilePath)
	if err != nil {
		t.Fatalf("failed to created test file: %s", err)
	}
	defer testfile.Close()

	// write some random data to the file
	dataBuf := make([]byte, 512)
	rand.Read(dataBuf)
	_, err = testfile.Write(dataBuf)
	if err != nil {
		t.Fatalf("write to test file failed: %s", err)
	}
	testfile.Close()

	reparseCreator := &wcifsReparsePointCreator{
		targetPath: "D:\\Containers\\testdata\\reparse",
	}

	tfi := &stdFileInfoProvider{
		root: tempDir,
	}

	err = reparseCreator.createWciReparsePoint(context.TODO(), filepath.Base(testfile.Name()), tfi)
	if err != nil {
		t.Fatalf("failed to crate reparse point: %s", err)
	}
}
