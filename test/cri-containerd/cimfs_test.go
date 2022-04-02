//go:build functional
// +build functional

package cri_containerd

import (
	"context"
	"fmt"
	"testing"

	runtime "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
)

func Test_MultipleContainers_WCOWHypervisor(t *testing.T) {
	requireFeatures(t, featureWCOWHypervisor)

	nanoserverMultilayerImage1 := "cplatpublic.azurecr.io/multilayer_nanoserver_1:ltsc2022"
	nanoserverMultilayerImage2 := "cplatpublic.azurecr.io/multilayer_nanoserver_2:ltsc2022"
	nanoserverMultilayerImage3 := "cplatpublic.azurecr.io/multilayer_nanoserver_3:ltsc2022"

	containerImages := []string{imageWindowsNanoserver, nanoserverMultilayerImage1, nanoserverMultilayerImage2, nanoserverMultilayerImage3}
	pullRequiredImages(t, containerImages)

	client := newTestRuntimeClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nContainers := len(containerImages)
	podIDs := make([]string, nContainers)
	containerIDs := make([]string, nContainers)

	for i := 0; i < nContainers; i++ {
		defer cleanupPod(t, client, ctx, &podIDs[i])
		defer cleanupContainer(t, client, ctx, &containerIDs[i])
	}

	for i := 0; i < nContainers; i++ {
		sandboxRequest := getRunPodSandboxRequest(t, wcowHypervisorRuntimeHandler)

		sandboxRequest.Config.Metadata.Name = fmt.Sprintf("%s-sandbox-%d", t.Name(), i)
		podIDs[i] = runPodSandbox(t, client, ctx, sandboxRequest)

		request := &runtime.CreateContainerRequest{
			PodSandboxId:  podIDs[i],
			SandboxConfig: sandboxRequest.Config,
			Config: &runtime.ContainerConfig{
				Metadata: &runtime.ContainerMetadata{
					Name: fmt.Sprintf("%s-container-%d", t.Name(), i),
				},
				Image: &runtime.ImageSpec{
					Image: containerImages[i],
				},
				// Hold this command open until killed (pause for Windows)
				Command: []string{
					"cmd",
					"/c",
					"ping",
					"-t",
					"127.0.0.1",
				},
			},
		}

		containerIDs[i] = createContainer(t, client, ctx, request)
		startContainer(t, client, ctx, containerIDs[i])
	}

	for i := 0; i < nContainers; i++ {
		verifyContainerExec(ctx, t, client, containerIDs[i])
	}
}

func Test_MultipleContainers_SamePod_WCOWHypervisor(t *testing.T) {
	requireFeatures(t, featureWCOWHypervisor)

	nanoserverMultilayerImage1 := "cplatpublic.azurecr.io/multilayer_nanoserver_1:ltsc2022"
	nanoserverMultilayerImage2 := "cplatpublic.azurecr.io/multilayer_nanoserver_2:ltsc2022"
	nanoserverMultilayerImage3 := "cplatpublic.azurecr.io/multilayer_nanoserver_3:ltsc2022"

	containerImages := []string{nanoserverMultilayerImage1, nanoserverMultilayerImage2, nanoserverMultilayerImage3}
	pullRequiredImages(t, containerImages)

	client := newTestRuntimeClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nContainers := len(containerImages)
	containerIDs := make([]string, nContainers)

	sandboxRequest := getRunPodSandboxRequest(t, wcowHypervisorRuntimeHandler)
	sandboxRequest.Config.Metadata.Name = fmt.Sprintf("%s-sandbox-%d", t.Name(), 0)
	podID := runPodSandbox(t, client, ctx, sandboxRequest)
	defer cleanupPod(t, client, ctx, &podID)

	for i := 0; i < nContainers; i++ {
		defer cleanupContainer(t, client, ctx, &containerIDs[i])
	}

	for i := 0; i < nContainers; i++ {

		request := &runtime.CreateContainerRequest{
			PodSandboxId:  podID,
			SandboxConfig: sandboxRequest.Config,
			Config: &runtime.ContainerConfig{
				Metadata: &runtime.ContainerMetadata{
					Name: fmt.Sprintf("%s-container-%d", t.Name(), i),
				},
				Image: &runtime.ImageSpec{
					Image: containerImages[i],
				},
				// Hold this command open until killed (pause for Windows)
				Command: []string{
					"cmd",
					"/c",
					"ping",
					"-t",
					"127.0.0.1",
				},
			},
		}

		containerIDs[i] = createContainer(t, client, ctx, request)
		startContainer(t, client, ctx, containerIDs[i])
	}

	for i := 0; i < nContainers; i++ {
		verifyContainerExec(ctx, t, client, containerIDs[i])
	}
}
