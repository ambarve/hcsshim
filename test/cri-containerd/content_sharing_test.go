//go:build windows && functional
// +build windows,functional

package cri_containerd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/namespaces"
	ctrdconfig "github.com/containerd/containerd/services/server/config"
	"github.com/pelletier/go-toml"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
)

// updateBoltConfig updates the bolt config section inside containerd config to have
// correct values for content sharing policy and snapshot sharing policy based on the
// input boolean parameters.
func updateBoltConfig(cfg *ctrdconfig.Config, contentSharing, snapshotSharing bool) error {
	var boltCfg ctrdconfig.BoltConfig

	boltCfg.ContentSharingPolicy = ctrdconfig.SharingPolicyShared
	if !contentSharing {
		boltCfg.ContentSharingPolicy = ctrdconfig.SharingPolicyIsolated
	}
	boltCfg.SnapshotSharingPolicy = ctrdconfig.SharingPolicyIsolated
	if snapshotSharing {
		boltCfg.SnapshotSharingPolicy = ctrdconfig.SharingPolicyShared
	}

	marshalledCfg, err := toml.Marshal(boltCfg)
	if err != nil {
		fmt.Errorf("failed to marshal bolt config: %s", err)
	}

	boltdata, err := toml.LoadBytes(marshalledCfg)
	if err != nil {
		fmt.Errorf("failed to convert marshalled data into toml tree: %s", err)
	}

	cfg.Plugins["bolt"] = *boltdata
	return nil
}

func createContainerdClientContext(t *testing.T, namespace string) (*containerd.Client, context.Context, error) {
	ctx := namespaces.WithNamespace(context.Background(), namespace)
	// Also include grpc namespace header so that namespace info is passed over during CRI API calls
	ctx = withGRPCNamespaceHeader(ctx, namespace)

	conn, err := createGRPCConn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("grpc connection failed: %s", err)
	}

	client, err := containerd.NewWithConn(conn, containerd.WithDefaultNamespace(namespace))
	if err != nil {
		return nil, nil, fmt.Errorf("create containerd client failed: %s", err)
	}

	return client, ctx, nil
}

// Create 3 namespaces, 1 common & 2 private. Pull an image into each of these namespaces
// and verify that snapshots are shared among all namespaces.
func Test_SnapshotSharing(t *testing.T) {
	cfg, err := loadContainerdConfigFile(tomlPath)
	if err != nil {
		t.Fatalf("failed to load containerd config: %s\n", err)
	}

	if err = updateBoltConfig(cfg, false, true); err != nil {
		t.Fatalf("failed to set bolt config: %s", err)
	}

	// use a temporary directory as containerd data directory
	tempDir := t.TempDir()
	cfg.Root = filepath.Join(tempDir, "root")
	cfg.State = filepath.Join(tempDir, "state")

	cm := NewContainerdManager(t, cfg)
	cm.init()
	defer cm.cleanup()

	// All of the following image are created from nanoserver:ltsc2022.  img1
	// has 4 layers, img2 has 6 layers and img3 has 4 layers. All of these images have
	// the common base layer of nanoserver:ltsc2022.  img2 is created by adding two
	// new layers on top of img1, so 4 bottom most layers are common between img1 &
	// img2. img3 shares just one layer with other images (other than the common
	// base layer).
	// When these images are pulled and snapshot sharing is enabled we should see
	// exactly 8 snapshots in the backend windows snapshotter. (1 for nanoserver base
	// layer, 3 unique from img1, 2 unique from img2 & 2 unique from img3).
	imgs := []string{
		"cplatpublic.azurecr.io/multilayer_nanoserver_1:ltsc2022",
		"cplatpublic.azurecr.io/multilayer_nanoserver_2:ltsc2022",
		"cplatpublic.azurecr.io/multilayer_nanoserver_3:ltsc2022",
	}

	testData := []struct {
		client   *containerd.Client
		ctx      context.Context
		ns       string
		nsLabels map[string]string
	}{
		{ns: "common", nsLabels: map[string]string{"containerd.io/namespace.shareable": "true"}},
		{ns: "private1", nsLabels: map[string]string{}},
		{ns: "private2", nsLabels: map[string]string{}},
	}

	for i := range testData {
		td := &testData[i]
		td.client, td.ctx, err = createContainerdClientContext(t, td.ns)
		if err != nil {
			t.Fatalf("failed to created containerd client & context: %s", err)
		}

		// create namespaces
		err = td.client.NamespaceService().Create(td.ctx, td.ns, td.nsLabels)
		if err != nil {
			t.Fatalf("failed to create namespace: %s", err)
		}

		_, err = td.client.Pull(td.ctx, imgs[i], containerd.WithPullUnpack)
		if err != nil {
			t.Fatalf("failed to pull image: %s", err)
		}
	}

	// verify that we have exactly 8 snapshots.
	// windows snapshotter directory
	snDir := filepath.Join(cfg.Root, "io.containerd.snapshotter.v1.windows", "snapshots")
	entries, err := os.ReadDir(snDir)
	if err != nil {
		t.Fatalf("failed to read snapshot directory: %s", err)
	}

	if len(entries) != 8 {
		t.Fatalf("expected exactly 8 snapshot directories")
	}

	for i := range testData {
		// remove image so that cleanup doesn't fail
		td := &testData[i]
		if err := td.client.ImageService().Delete(td.ctx, imgs[i], images.SynchronousDelete()); err != nil {
			t.Logf("failed to remove image %s: %s", imgs[i], err)
		}
	}

	// Give GC sometime to cleanup snapshots
	time.Sleep(5 * time.Second)
}

// Test_SnapshotSharingCRI creates 3 namespaces (1 shared, 2 private), pulls an image into each of them
// and then runs a container with each of those images.
func DisabledTest_SnapshotSharingCRI(t *testing.T) {
	cfg, err := loadContainerdConfigFile(tomlPath)
	if err != nil {
		t.Fatalf("failed to load containerd config: %s\n", err)
	}

	if err = updateBoltConfig(cfg, false, true); err != nil {
		t.Fatalf("failed to set bolt config: %s", err)
	}

	// use a temporary directory as containerd data directory
	tempDir := t.TempDir()
	cfg.Root = filepath.Join(tempDir, "root")
	cfg.State = filepath.Join(tempDir, "state")

	t.Logf("rootdir: %s\n", cfg.Root)
	cm := NewContainerdManager(t, cfg)
	cm.init()
	defer cm.cleanup()

	//var err error

	imgs := []string{
		"cplatpublic.azurecr.io/multilayer_nanoserver_1:ltsc2022",
		"cplatpublic.azurecr.io/multilayer_nanoserver_2:ltsc2022",
		"cplatpublic.azurecr.io/multilayer_nanoserver_3:ltsc2022",
	}

	testData := []struct {
		client   *containerd.Client
		ctx      context.Context
		ns       string
		nsLabels map[string]string
	}{
		{ns: "common", nsLabels: map[string]string{"containerd.io/namespace.shareable": "true"}},
		{ns: "private1", nsLabels: map[string]string{}},
		{ns: "private2", nsLabels: map[string]string{}},
	}

	imgPullOpts := WithSandboxLabels(map[string]string{"sandbox-platform": "windows/amd64"})
	for i := range testData {
		td := &testData[i]
		td.client, td.ctx, err = createContainerdClientContext(t, td.ns)
		if err != nil {
			t.Fatalf("failed to created containerd client & context: %s", err)
		}

		// create namespaces
		if err = td.client.NamespaceService().Create(td.ctx, td.ns, td.nsLabels); err != nil {
			t.Fatalf("failed to create namespace: %s", err)
		}

		imgCli := runtime.NewImageServiceClient(td.client.Conn())
		pullRequiredImagesWithOptions(td.ctx, imgCli, t, imgs[i:i+1], imgPullOpts)
	}

	for idx := range testData {
		// remove image so that cleanup doesn't fail
		// defer func(idx int) {
		td := &testData[idx]
		if err := td.client.ImageService().Delete(td.ctx, imgs[idx], images.SynchronousDelete()); err != nil {
			t.Logf("failed to remove image %s: %s", imgs[idx], err)
		}
		t.Logf("cleanup for %s done\n", imgs[idx])
		// }(i)
	}

	// // verify that common namespace shows only 1 image, while private namespaces show 2 images each.
	// imgReq := &runtime.ListImagesRequest{}
	// for i := range testData {
	// 	td := &testData[i]
	// 	imgRes, err := runtime.NewImageServiceClient(td.client.Conn()).ListImages(td.ctx, imgReq)
	// 	if err != nil {
	// 		t.Fatalf("failed to get image list: %s", err)
	// 	}

	// 	t.Logf("%s %d images: %+v\n", time.Now(), i, imgRes)
	// 	if i == 0 && len(imgRes.Images) != 1 {
	// 		t.Fatalf("expected exactly 1 image in common namespace, found %d", len(imgRes.Images))
	// 	} else if len(imgRes.Images) != 2 {
	// 		t.Fatalf("expected exactly 2 images in private namespace, found %d", len(imgRes.Images))
	// 	}
	// }

	// verify that we have exactly 8 snapshots.
	// windows snapshotter directory
	// snDir := filepath.Join(cfg.Root, "io.containerd.snapshotter.v1.windows", "snapshots")
	// entries, err := os.ReadDir(snDir)
	// if err != nil {
	// 	t.Fatalf("failed to read snapshot directory: %s", err)
	// }

	// if len(entries) != 8 {
	// 	t.Fatalf("expected exactly 8 snapshot directories")
	// }
}

func Test_Spoof(t *testing.T) {
	cfg, err := loadContainerdConfigFile(tomlPath)
	if err != nil {
		t.Fatalf("failed to load containerd config: %s\n", err)
	}

	if err = updateBoltConfig(cfg, false, true); err != nil {
		t.Fatalf("failed to set bolt config: %s", err)
	}

	// use a temporary directory as containerd data directory
	tempDir := t.TempDir()
	cfg.Root = filepath.Join(tempDir, "root")
	cfg.State = filepath.Join(tempDir, "state")

	cm := NewContainerdManager(t, cfg)
	cm.init()
	defer cm.cleanup()

	// All of the following image are created from nanoserver:ltsc2022.  img1
	// has 4 layers, img2 has 6 layers and img3 has 4 layers. All of these images have
	// the common base layer of nanoserver:ltsc2022.  img2 is created by adding two
	// new layers on top of img1, so 4 bottom most layers are common between img1 &
	// img2. img3 shares just one layer with other images (other than the common
	// base layer).
	// When these images are pulled and snapshot sharing is enabled we should see
	// exactly 8 snapshots in the backend windows snapshotter. (1 for nanoserver base
	// layer, 3 unique from img1, 2 unique from img2 & 2 unique from img3).
	imgs := []string{
		"victimacr.azurecr.io/victim:latest",
	}

	testData := []struct {
		client   *containerd.Client
		ctx      context.Context
		ns       string
		nsLabels map[string]string
	}{
		{ns: "common", nsLabels: map[string]string{"containerd.io/namespace.shareable": "true"}},
		{ns: "private1", nsLabels: map[string]string{}},
		{ns: "private2", nsLabels: map[string]string{}},
	}

	for i := range testData {
		td := &testData[i]
		td.client, td.ctx, err = createContainerdClientContext(t, td.ns)
		if err != nil {
			t.Fatalf("failed to created containerd client & context: %s", err)
		}

		// create namespaces
		err = td.client.NamespaceService().Create(td.ctx, td.ns, td.nsLabels)
		if err != nil {
			t.Fatalf("failed to create namespace: %s", err)
		}

		_, err = td.client.Pull(td.ctx, imgs[i], containerd.WithPullUnpack)
		if err != nil {
			t.Fatalf("failed to pull image: %s", err)
		}
	}

	// verify that we have exactly 8 snapshots.
	// windows snapshotter directory
	snDir := filepath.Join(cfg.Root, "io.containerd.snapshotter.v1.windows", "snapshots")
	entries, err := os.ReadDir(snDir)
	if err != nil {
		t.Fatalf("failed to read snapshot directory: %s", err)
	}

	if len(entries) != 8 {
		t.Fatalf("expected exactly 8 snapshot directories")
	}

	for i := range testData {
		// remove image so that cleanup doesn't fail
		td := &testData[i]
		if err := td.client.ImageService().Delete(td.ctx, imgs[i], images.SynchronousDelete()); err != nil {
			t.Logf("failed to remove image %s: %s", imgs[i], err)
		}
	}

	// Give GC sometime to cleanup snapshots
	time.Sleep(5 * time.Second)
}
