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
	ctrdimages "github.com/containerd/containerd/images"
	"github.com/containerd/containerd/namespaces"
	ctrdconfig "github.com/containerd/containerd/services/server/config"
	criconfig "github.com/containerd/cri/pkg/config"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pelletier/go-toml"
)

// getPluginConfig retrieves the toml tree of the configuration of the plugin named `pluginName` from the
// containerd config and decodes it into the `pluginConfig` struct. `pluginConfig` must be a point to the
// struct that represents the configuration of this plugin.
func getPluginConfig(ctrdcfg *ctrdconfig.Config, pluginName string, pluginConfig interface{}) error {
	critomltree := ctrdcfg.Plugins[pluginName]
	if err := critomltree.Unmarshal(pluginConfig); err != nil {
		return err
	}
	return nil
}

// setPluginConfig marshalls the given `pluginConfig` into the `toml.TomlTree` format and puts it inside the
// provided containerd config.
func setPluginConfig(ctrdcfg *ctrdconfig.Config, pluginName string, pluginConfig interface{}) error {
	cfgbytes, err := toml.Marshal(pluginConfig)
	if err != nil {
		return err
	}

	cfgtree, err := toml.LoadBytes(cfgbytes)
	if err != nil {
		return err
	}

	ctrdcfg.Plugins[pluginName] = *cfgtree
	return nil
}

func createContainerdClientAndContext(t *testing.T, namespace string) (*containerd.Client, context.Context, error) {
	ctx := namespaces.WithNamespace(context.Background(), namespace)

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

func getImageLayers(ctx context.Context, ctrdClient *containerd.Client, img containerd.Image) ([]ocispec.Descriptor, error) {
	manifest, err := ctrdimages.Manifest(ctx, ctrdClient.ContentStore(), img.Target(), img.Platform())
	if err != nil {
		return nil, err
	}
	return manifest.Layers, nil
}

func Test_ContentSharing(t *testing.T) {
	cfg, err := loadContainerdConfigFile(tomlPath)
	if err != nil {
		t.Fatalf("failed to load containerd config: %s\n", err)
	}

	// update cri config
	cricfg := &criconfig.PluginConfig{}
	if err := getPluginConfig(cfg, "cri", cricfg); err != nil {
		t.Fatalf("failed to parse cri config: %s", err)
	}
	cricfg.ContainerdConfig.EnableImagePullRecord = true
	if err := setPluginConfig(cfg, "cri", cricfg); err != nil {
		t.Fatalf("failed to parse cri config: %s", err)
	}

	// use a temporary directory as containerd data directory
	// tempDir := t.TempDir()
	// cfg.Root = filepath.Join(tempDir, "root")
	// cfg.State = filepath.Join(tempDir, "state")

	cm := NewContainerdManager(t, cfg)
	cm.init()
	defer cm.cleanup()

	// All of the following image are created with base layer as nanoserver:ltsc2022. img1 has a total of
	// 4 layers, img2 has a total of 6 layers and img3 has a total of 4 layers. img2 is created by adding
	// two new layers on top of img1, so 4 bottom most layers are common between img1 & img2. img3 shares
	// just one layer with other images (other than the common base layer).  There are a total of 8 unique
	// layers among these 3 images, when we pull img1 we should see new content in the ContentStore for 4
	// of its layers, when we pull img2 the first 4 layers should be reused from the ContentStore and 2
	// new layers should be pulled. So we verify that the updatedAt timestamp for the first 4 layers is
	// older than the time at which we initiated pull for img2. We do similar validation after pull of
	// img3.
	imgRefs := []string{
		"cplatpublic.azurecr.io/multilayer_nanoserver_1:ltsc2022",
		"cplatpublic.azurecr.io/multilayer_nanoserver_2:ltsc2022",
		"cplatpublic.azurecr.io/multilayer_nanoserver_3:ltsc2022",
	}

	// get a timestamp before we pull the image
	compareTimeStamp := time.Now()

	ctrdClient, ctx, err := createContainerdClientAndContext(t, "k8s.io")
	if err != nil {
		t.Fatalf("failed to create containerd client & context: %s", err)
	}

	for idx, ref := range imgRefs {
		pullRequiredImages(t, imgRefs[idx:idx+1])
		// defer a call in case of errors
		defer removeImages(t, imgRefs[idx:idx+1])

		img, err := ctrdClient.GetImage(ctx, ref)
		if err != nil {
			t.Fatalf("failed to get image %s: %s", ref, err)
		}

		layers, err := getImageLayers(ctx, ctrdClient, img)
		if err != nil {
			t.Fatalf("failed to get image layers %s", err)
		}

		// index of the first layer that will be newly downloaded because it doesn't already exist
		newLayerIdx := 0
		switch idx {
		case 1:
			newLayerIdx = 4 // First 4 layers of img2 are exactly same as that of img1
		case 2:
			newLayerIdx = 2 // First 2 layers of img3 are exactly same that of img1
		}

		for _, l := range layers[newLayerIdx:] {
			cinfo, err := ctrdClient.ContentStore().Info(ctx, l.Digest)
			if err != nil {
				t.Fatalf("Failed to get info for content %s: %s", l.Digest, err)
			}
			if !compareTimeStamp.Before(cinfo.UpdatedAt) {
				t.Fatalf("Timestamp for content %s, shouldn't update", l.Digest)
			}
		}

		compareTimeStamp = time.Now()
		// Give some time so that compareTimeStamp.Before() check isn't too close.
		time.Sleep(100 * time.Millisecond)
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

	removeImages(t, imgRefs)

	// Give GC sometime to cleanup snapshots
	time.Sleep(10 * time.Second)
}
