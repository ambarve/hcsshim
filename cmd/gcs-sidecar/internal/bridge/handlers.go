//go:build windows
// +build windows

package bridge

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/Microsoft/hcsshim/cmd/gcs-sidecar/internal/windowssecuritypolicy"
	"github.com/Microsoft/hcsshim/hcn"
	"github.com/Microsoft/hcsshim/internal/fsformatter"
	hcsschema "github.com/Microsoft/hcsshim/internal/hcs/schema2"
	"github.com/Microsoft/hcsshim/internal/protocol/guestrequest"
	"github.com/Microsoft/hcsshim/internal/protocol/guestresource"
	"github.com/Microsoft/hcsshim/internal/windevice"
	"github.com/Microsoft/hcsshim/pkg/cimfs"
	"github.com/pkg/errors"
)

// Current intent of these handler functions is to call the security policy
// enforcement code as needed and return nil if the operation is allowed.
// Else error is returned.
// Also, these handler functions decide if request needs to be forwarded
// to inbox GCS or not. Request is forwarded asynchronously.
func (b *Bridge) createContainer(req *request) error {
	var err error = nil
	var r containerCreate
	var containerConfig json.RawMessage

	r.ContainerConfig.Value = &containerConfig
	if err = json.Unmarshal(req.message, &r); err != nil {
		log.Printf("failed to unmarshal rpcCreate: %v", req)
		// TODO: Send valid error response back to the sender before closing bridge
		return fmt.Errorf("failed to unmarshal rpcCreate: %v", req)
	}

	// containerCreate.ContainerConfig can be of type uvnConfig or hcsschema.HostedSystem
	var uvmConfig uvmConfig
	var hostedSystemConfig hcsschema.HostedSystem
	if err = json.Unmarshal(containerConfig, &uvmConfig); err == nil {
		systemType := uvmConfig.SystemType
		timeZoneInformation := uvmConfig.TimeZoneInformation
		log.Printf("rpcCreate: \n ContainerCreate{ requestBase: %v, uvmConfig: {systemType: %v, timeZoneInformation: %v}}", r.requestBase, systemType, timeZoneInformation)
		// TODO: call policy enforcement points once ready
		// err = call policyEnforcer
		// return on err
	} else if err = json.Unmarshal(containerConfig, &hostedSystemConfig); err == nil {
		schemaVersion := hostedSystemConfig.SchemaVersion
		container := hostedSystemConfig.Container
		log.Printf("rpcCreate: \n ContainerCreate{ requestBase: %v, ContainerConfig: {schemaVersion: %v, container: %v}}", r.requestBase, schemaVersion, container)
		// TODO: call policy enforcement points once ready
		// err = call policyEnforcer
		// return on err
	} else {
		log.Printf("createContainer: invalid containerConfig type. Request: %v", req)
		// TODO: Send valid error response back to the sender before closing bridge
		return fmt.Errorf("createContainer: invalid containerConfig type. Request: %v", r)
	}

	// If we've reached here, means the policy has allowed operation.
	// So forward msg to inbox GCS.
	b.sendToGCSCh <- *req

	return err
}

func (b *Bridge) startContainer(req *request) error {
	var r requestBase
	var err error
	if err = json.Unmarshal(req.message, &r); err != nil {
		return fmt.Errorf("failed to unmarshal rpcStart: %v", req)
	}
	log.Printf("rpcStart: \n requestBase: %v", r)

	// TODO: call policy enforcement points once ready
	// err = call policyEnforcer
	// return on err

	b.sendToGCSCh <- *req

	return nil
}

func (b *Bridge) shutdownGraceful(req *request) error {
	var r requestBase
	var err error = nil
	var ctx context.Context
	if err = json.Unmarshal(req.message, &r); err != nil {
		return fmt.Errorf("failed to unmarshal rpcShutdownGraceful: %v", req)
	}
	log.Printf("rpcShutdownGraceful: \n requestBase: %v", r)

	b.PolicyEnforcer.securityPolicyEnforcer.EnforceShutdownContainerPolicy(ctx, r.ContainerID)
	if err != nil {
		return fmt.Errorf("rpcShudownGraceful operation not allowed: %v", err)
	}

	b.sendToGCSCh <- *req

	return nil
}

func (b *Bridge) shutdownForced(req *request) error {
	var r requestBase
	var err error = nil
	if err = json.Unmarshal(req.message, &r); err != nil {
		return fmt.Errorf("failed to unmarshal rpcShutdownForced: %v", req)
	}
	log.Printf("rpcShutdownForced: \n requestBase: %v", r)

	/*
		containerdID := r.ContainerdID
		b.securityPolicyEnforcer.EnforceShutdownContainerPolicy(ctx, containerID)
		if err != nil {
			return fmt.Errorf("rpcShudownGraceful operation not allowed: %v", err)
		}
	*/

	b.sendToGCSCh <- *req

	return nil
}

func (b *Bridge) executeProcess(req *request) error {
	var err error = nil
	var r containerExecuteProcess
	var processParamSettings json.RawMessage
	r.Settings.ProcessParameters.Value = &processParamSettings
	if err = json.Unmarshal(req.message, &r); err != nil {
		return fmt.Errorf("failed to unmarshal rpcExecuteProcess: %v", req)
	}
	containerID := r.requestBase.ContainerID
	stdioRelaySettings := r.Settings.StdioRelaySettings
	vsockStdioRelaySettings := r.Settings.VsockStdioRelaySettings

	var processParams hcsschema.ProcessParameters
	if err = json.Unmarshal(processParamSettings, &processParams); err != nil {
		log.Printf("rpcExecProcess: invalid params type for request %v", r.Settings)
		return fmt.Errorf("rpcExecProcess: invalid params type for request %v", r.Settings)
	}

	log.Printf("rpcExecProcess: \n containerID: %v, schema1.ProcessParameters{ params: %v, stdioRelaySettings: %v, vsockStdioRelaySettings: %v }", containerID, processParams, stdioRelaySettings, vsockStdioRelaySettings)
	// err = call policy enforcer

	b.sendToGCSCh <- *req

	return err
}

func (b *Bridge) waitForProcess(req *request) error {
	var r containerWaitForProcess
	var err error = nil

	if err = json.Unmarshal(req.message, &r); err != nil {
		return fmt.Errorf("failed to unmarshal waitForProcess: %v", req)
	}
	log.Printf("rpcWaitForProcess: \n containerWaitForProcess{ requestBase: %v, processID: %v, timeoutInMs: %v }", r.requestBase, r.ProcessID, r.TimeoutInMs)

	// enforcement

	b.sendToGCSCh <- *req

	return nil
}

func (b *Bridge) signalProcess(req *request) error {
	var err error
	var r containerSignalProcess
	var rawOpts json.RawMessage
	r.Options = &rawOpts

	if err = json.Unmarshal(req.message, &r); err != nil {
		return fmt.Errorf("failed to unmarshal rpcSignalProcess: %v", req)
	}

	log.Printf("rpcSignalProcess: request %v", r)

	var wcowOptions guestresource.SignalProcessOptionsWCOW
	if rawOpts == nil {
		b.sendToGCSCh <- *req
		return nil
	} else if err = json.Unmarshal(rawOpts, &wcowOptions); err != nil {
		log.Printf("rpcSignalProcess: invalid Options type for request %v", r)
		return fmt.Errorf("rpcSignalProcess: invalid Options type for request %v", r)
	}
	log.Printf("rpcSignalProcess: \n containerSignalProcess{ requestBase: %v, processID: %v, Options: %v }", r.requestBase, r.ProcessID, wcowOptions)

	// placeholder for calling policy enforcer and return error message
	err = signalProcess(r.ContainerID, r.ProcessID, wcowOptions.Signal)
	if err != nil {
		return fmt.Errorf("waitForProcess not allowed due to policy")
	}

	// If we've reached here, means the policy has allowed it.
	// So forward msg to inbox GCS.
	b.sendToGCSCh <- *req

	return nil
}

func (b *Bridge) resizeConsole(req *request) error {
	var r containerResizeConsole
	var err error = nil

	if err = json.Unmarshal(req.message, &r); err != nil {
		return fmt.Errorf("failed to unmarshal rpcSignalProcess: %v", req)
	}
	log.Printf("rpcResizeConsole: \n containerResizeConsole{ requestBase: %v, processID: %v, height: %v, width: %v }", r.requestBase, r.ProcessID, r.Height, r.Width)

	// placeholder for calling policy enforcer and return error message
	err = resizeConsole(r.ContainerID, r.Height, r.Width)
	if err != nil {
		return fmt.Errorf("waitForProcess not allowed due to policy")
	}

	// If we've reached here, means the policy has allowed it.
	// So forward msg to inbox GCS.
	b.sendToGCSCh <- *req

	return nil
}

func (b *Bridge) getProperties(req *request) error {
	// TODO: This has containerGetProperties and containerGetPropertiesV2. Need to find a way to differentiate!
	/*
		var r containerGetProperties
		if err := json.Unmarshal(req.message, &r); err != nil {
			return fmt.Errorf("failed to unmarshal rpcSignalProcess: %v", req)
		}
	*/
	// TODO: Error out if v1 schema is being used as we will not support bringing up sidecar-gcs there
	b.sendToGCSCh <- *req
	return nil
}

func (b *Bridge) unmarshalModifySettingsAndForward(req *request) error {
	log.Printf("\n, unmarshalModifySettingsAndForward:Header {Type: %v Size: %v ID: %v }\n msg: %v \n", getMessageType(req.header), getMessageSize(req.header), getMessageID(req.header), string(req.message))
	// skipSendToGCS := false
	var r containerModifySettings
	var requestRawSettings json.RawMessage
	r.Request = &requestRawSettings
	if err := json.Unmarshal(req.message, &r); err != nil {
		return fmt.Errorf("failed to unmarshal rpcModifySettings: %v", req)
	}

	var modifyGuestSettingsRequest guestrequest.ModificationRequest
	var rawGuestRequest json.RawMessage
	modifyGuestSettingsRequest.Settings = &rawGuestRequest
	if err := json.Unmarshal(requestRawSettings, &modifyGuestSettingsRequest); err != nil {
		log.Printf("invalid rpcModifySettings ModificationRequest request %v", r)
		return fmt.Errorf("invalid rpcModifySettings ModificationRequest request %v", r)
	}
	log.Printf("rpcModifySettings: ModificationRequest %v\n", modifyGuestSettingsRequest)

	guestResourceType := modifyGuestSettingsRequest.ResourceType
	guestRequestType := modifyGuestSettingsRequest.RequestType // add, remove, preadd, update
	if guestResourceType == "" {
		modifyGuestSettingsRequest.RequestType = guestrequest.RequestTypeAdd
	}
	log.Printf("rpcModifySettings: guestRequest.ModificationRequest { resourceType: %v \n, requestType: %v", guestResourceType, guestRequestType)

	// TODO: Do we need to validate request types?
	switch guestRequestType {
	case guestrequest.RequestTypeAdd:

	case guestrequest.RequestTypeRemove:

	case guestrequest.RequestTypePreAdd:

	case guestrequest.RequestTypeUpdate:
	default:
		log.Printf("\n Invald guestRequestType: %v", guestRequestType)
		return fmt.Errorf("invald guestRequestType %v", guestRequestType)
	}

	if guestResourceType != "" {
		switch guestResourceType {
		case guestresource.ResourceTypeCWCOWCombinedLayers:
			settings := &guestresource.CWCOWCombinedLayers{}
			if err := json.Unmarshal(rawGuestRequest, settings); err != nil {
				log.Printf("invalid ResourceTypeWCOWCombinedLayers request %v", r)
				return fmt.Errorf("invalid ResourceTypeCombinedLayers request %v", r)
			}
			containerID := settings.ContainerID
			log.Printf(", CWCOWCombinedLayers {ContainerID: %v {ContainerRootPath: %v, Layers: %v, ScratchPath: %v}} \n",
				containerID, settings.CombinedLayers.ContainerRootPath, settings.CombinedLayers.Layers, settings.CombinedLayers.ScratchPath)

			// check that this is not denied by policy
			// TODO: modify gcs-sidecar code to pass context across all calls
			// TODO: Update modifyCombinedLayers with verified CimFS API
			var ctx context.Context
			policy_err := modifyCombinedLayers(ctx, containerID, guestRequestType, settings.CombinedLayers, b.PolicyEnforcer.securityPolicyEnforcer)
			if policy_err != nil {
				return fmt.Errorf("CimFS layer mount is denied by policy: %v", r)
			}

			// reconstruct WCOWCombinedLayers{} and req before forwarding to GCS
			// as GCS does not understand containerID in CombinedLayers request
			modifyGuestSettingsRequest.ResourceType = guestresource.ResourceTypeCombinedLayers
			modifyGuestSettingsRequest.Settings = settings.CombinedLayers
			r.Request = modifyGuestSettingsRequest
			buf, err := json.Marshal(r)
			if err != nil {
				return fmt.Errorf("failed to marshal rpcModifySettings: %v", req)
			}

			var newRequest request
			newRequest.header = req.header
			setRequestSize(&newRequest, uint32(len(buf)))
			newRequest.message = buf
			req = &newRequest

		case guestresource.ResourceTypeCombinedLayers:
			settings := &guestresource.WCOWCombinedLayers{}
			if err := json.Unmarshal(rawGuestRequest, settings); err != nil {
				log.Printf("invalid ResourceTypeCombinedLayers request %v", r)
				return fmt.Errorf("invalid ResourceTypeCombinedLayers request %v", r)
			}

			log.Printf(", WCOWCombinedLayers {ContainerRootPath: %v, Layers: %v, ScratchPath: %v} \n", settings.ContainerRootPath, settings.Layers, settings.ScratchPath)

		case guestresource.ResourceTypeNetworkNamespace:
			settings := &hcn.HostComputeNamespace{}
			if err := json.Unmarshal(rawGuestRequest, settings); err != nil {
				log.Printf("invalid ResourceTypeNetworkNamespace request %v", r)
				return fmt.Errorf("invalid ResourceTypeNetworkNamespace request %v", r)
			}

			log.Printf(", HostComputeNamespaces { %v} \n", settings)

		case guestresource.ResourceTypeNetwork:
			// following valid only for osversion.Build() >= osversion.RS5
			// since Cwcow is available only for latest versions this is ok
			settings := &guestrequest.NetworkModifyRequest{}
			if err := json.Unmarshal(rawGuestRequest, settings); err != nil {
				log.Printf("invalid ResourceTypeNetwork request %v", r)
				return fmt.Errorf("invalid ResourceTypeNetwork request %v", r)
			}

			log.Printf(", NetworkModifyRequest { %v} \n", settings)

		case guestresource.ResourceTypeWCOWBlockCims:
			// This is request to mount the merged cim at given volumeGUID
			wcowBlockCimMounts := &guestresource.WCOWBlockCIMMounts{}
			if err := json.Unmarshal(rawGuestRequest, wcowBlockCimMounts); err != nil {
				log.Printf("invalid ResourceTypeWCOWBlockCims request %v", r)
				return fmt.Errorf("invalid ResourceTypeWCOWBlockCims request %v", r)
			}
			log.Printf(", WCOWBlockCIMMounts { %v} \n", wcowBlockCimMounts)

			// The block device takes some time to show up, temporary hack
			time.Sleep(1 * time.Second)

			var layerCIMs []*cimfs.BlockCIM
			ctx := context.Background()
			for _, blockCimDevice := range wcowBlockCimMounts.BlockCIMs {
				// Get the scsi device path for the blockCim lun
				scsiDevPath, _, err := windevice.GetScsiDevicePathAndDiskNumberFromControllerLUN(
					ctx,
					0, /* controller is always 0 for wcow */
					uint8(blockCimDevice.Lun))
				if err != nil {
					log.Printf("err getting scsiDevPath: %v", r)
					return err
				}
				layerCim := cimfs.BlockCIM{
					Type:      cimfs.BlockCIMTypeDevice,
					BlockPath: scsiDevPath,
					CimName:   blockCimDevice.CimName,
				}
				layerCIMs = append(layerCIMs, &layerCim)

			}
			if len(layerCIMs) > 1 {
				// Get the topmost merge CIM and invoke the MountMergedBlockCIMs
				_, err := cimfs.MountMergedBlockCIMs(layerCIMs[0], layerCIMs[1:], wcowBlockCimMounts.MountFlags, wcowBlockCimMounts.VolumeGuid)
				if err != nil {
					return fmt.Errorf("error mounting merged block cims: %v", err)
				}
			} else {
				_, err := cimfs.Mount(filepath.Join(layerCIMs[0].BlockPath, layerCIMs[0].CimName), wcowBlockCimMounts.VolumeGuid, wcowBlockCimMounts.MountFlags)
				if err != nil {
					return fmt.Errorf("error mounting merged block cims: %v", err)
				}
			}

			// Send reply back to hcsshim
			err := b.sendReplyToShim(rpcModifySettings, *req)
			if err != nil {
				log.Printf("error sending reply back to hcsshim from ResourceTypeWCOWBlockCims")
				return fmt.Errorf("error sending reply back to hcsshim from ResourceTypeWCOWBlockCims: %v", err)
			}
			return nil

		case guestresource.ResourceTypeMappedVirtualDiskForContainerScratch:
			wcowMappedVirtualDisk := &guestresource.WCOWMappedVirtualDisk{}
			if err := json.Unmarshal(rawGuestRequest, wcowMappedVirtualDisk); err != nil {
				log.Printf("invalid ResourceTypeMappedVirtualDisk request %v", r)
				return fmt.Errorf("invalid ResourceTypeMappedVirtualDisk request %v", r)
			}
			log.Printf(", wcowMappedVirtualDisk { %v} \n", wcowMappedVirtualDisk)

			// 1. First call fsFormatter to refs format the scratch disk.
			// This will return the volume path of the mounted scratch.
			// 2. Create symlink between the containerpath and volume path
			// returned by fsformatter
			// 3. TODO(kiashok) Test if we need to forward this request to the inbox GCS as all?
			// 4. Need to enforce policy before calling into fsFormatter

			// fsFormatter understands only virtualDevObjectPathFormat. Therefore fetch the
			// disk number for the corresponding lun
			ctx := context.Background()
			log.Printf("\n calling scsi ioctl to get disk path")
			var diskNumber uint64
			var err error
			// It could take a few seconds for the attached scsi disk
			// to show up inside the UVM. Therefore adding retry logic
			// with delay here.
			for i := 0; i < 5; i++ {
				time.Sleep(5 * time.Second)
				_, diskNumber, err = windevice.GetScsiDevicePathAndDiskNumberFromControllerLUN(ctx,
					0, /* Only one controller allowed in wcow hyperv */
					uint8(wcowMappedVirtualDisk.Lun))
				if err != nil {
					if i == 5 {
						// bail out
						log.Printf("error getting diskNumber for LUN %d, err : %v", wcowMappedVirtualDisk.Lun, err)
						return fmt.Errorf("error getting diskNumber for LUN %d", wcowMappedVirtualDisk.Lun)
					}
					continue
				} else {
					log.Printf("DiskNumber of lun %d is:  %d", wcowMappedVirtualDisk.Lun, diskNumber)
				}
			}
			diskPath := fmt.Sprintf(fsformatter.VirtualDevObjectPathFormat, diskNumber)
			log.Printf("\n diskPath: %v, diskNumber: %v ", diskPath, diskNumber)
			mountedVolumePath, err := windevice.InvokeFsFormatter(ctx, diskPath)
			if err != nil {
				log.Printf("\n InvokeFsFormatter returned err: %v", err)
				return err
			}
			log.Printf("\n mountedVolumePath returned from InvokeFsFormatter: %v", mountedVolumePath)

			// Just forward the req as is to the inbox gcs and let it retrive the volume.
			// While forwarding request to inbox gcs, make sure to replace
			// the resourceType to ResourceTypeMappedVirtualDisk that it
			// understands
			modifyGuestSettingsRequest.ResourceType = guestresource.ResourceTypeMappedVirtualDisk
			r.Request = modifyGuestSettingsRequest
			buf, err := json.Marshal(r)
			if err != nil {
				return fmt.Errorf("failed to marshal rpcModifySettings: %v", req)
			}

			var newRequest request
			newRequest.header = req.header
			setRequestSize(&newRequest, uint32(len(buf)))
			newRequest.message = buf
			req = &newRequest

		case guestresource.ResourceTypeMappedVirtualDisk:
			wcowMappedVirtualDisk := &guestresource.WCOWMappedVirtualDisk{}
			if err := json.Unmarshal(rawGuestRequest, wcowMappedVirtualDisk); err != nil {
				log.Printf("invalid ResourceTypeMappedVirtualDisk request %v", r)
				return fmt.Errorf("invalid ResourceTypeMappedVirtualDisk request %v", r)
			}

			var ctx context.Context
			policy_err := modifyMappedVirtualDisk(ctx, guestRequestType, wcowMappedVirtualDisk, b.PolicyEnforcer.securityPolicyEnforcer)
			if policy_err != nil {
				return fmt.Errorf("Mount device denied by policy %v", r)
			}
			log.Printf(", wcowMappedVirtualDisk { %v} \n", wcowMappedVirtualDisk)

		case guestresource.ResourceTypeHvSocket:
			hvSocketAddress := &hcsschema.HvSocketAddress{}
			if err := json.Unmarshal(rawGuestRequest, hvSocketAddress); err != nil {
				log.Printf("invalid ResourceTypeHvSocket request %v", r)
				return fmt.Errorf("invalid ResourceTypeHvSocket request %v", r)
			}

			log.Printf(", hvSocketAddress { %v} \n", hvSocketAddress)

		case guestresource.ResourceTypeSecurityPolicy:
			securityPolicyRequest := &guestresource.WCOWConfidentialOptions{}
			if err := json.Unmarshal(rawGuestRequest, securityPolicyRequest); err != nil {
				log.Printf("invalid ResourceTypeSecurityPolicy request %v", r)
				return fmt.Errorf("invalid ResourceTypeSecurityPolicy request %v", r)
			}

			log.Printf(", WCOWConfidentialOptions: { %v} \n", securityPolicyRequest)
			_ = b.PolicyEnforcer.SetWCOWConfidentialUVMOptions( /*ctx, */ securityPolicyRequest)
			// skipSendToGCS = true
			// send response back to shim
			log.Printf("\n early response to hcsshim? \n")
			err := b.sendReplyToShim(rpcModifySettings, *req)
			if err != nil {
				//
				log.Printf("error sending early reply back to hcsshim")
				err = fmt.Errorf("error sending early reply back to hcsshim")
				return err
			}
			return nil
			//return err, skipSendToGCS
		default:
			// invalid
			log.Printf("\n Invald modifySettingsRequest: %v", guestResourceType)
			return fmt.Errorf("invald modifySettingsRequest: %v", guestResourceType)
		}
	}

	// If we are here, there is no error and we want to
	// forward the message to inbox GCS
	b.sendToGCSCh <- *req

	return nil
	//, skipSendToGCS
}

func newInvalidRequestTypeError(rt guestrequest.RequestType) error {
	return errors.Errorf("the RequestType %q is not supported", rt)
}

func modifyMappedVirtualDisk(
	ctx context.Context,
	rt guestrequest.RequestType,
	mvd *guestresource.WCOWMappedVirtualDisk,
	securityPolicy windowssecuritypolicy.SecurityPolicyEnforcer,
) (err error) {
	switch rt {
	case guestrequest.RequestTypeAdd:
		// TODO: Modify and update this with verified Cims API
		return securityPolicy.EnforceDeviceMountPolicy(ctx, mvd.ContainerPath, "hash")
	case guestrequest.RequestTypeRemove:
		// TODO: Modify and update this with verified Cims API
		return securityPolicy.EnforceDeviceUnmountPolicy(ctx, mvd.ContainerPath)
	default:
		return newInvalidRequestTypeError(rt)
	}
}

func modifyCombinedLayers(
	ctx context.Context,
	containerID string,
	rt guestrequest.RequestType,
	cl guestresource.WCOWCombinedLayers,
	securityPolicy windowssecuritypolicy.SecurityPolicyEnforcer,
) (err error) {
	switch rt {
	case guestrequest.RequestTypeAdd:
		layerPaths := make([]string, len(cl.Layers))
		for i, layer := range cl.Layers {
			layerPaths[i] = layer.Path
		}
		return securityPolicy.EnforceOverlayMountPolicy(ctx, containerID, layerPaths, cl.ContainerRootPath)
	case guestrequest.RequestTypeRemove:
		return securityPolicy.EnforceOverlayUnmountPolicy(ctx, cl.ContainerRootPath)
	default:
		return newInvalidRequestTypeError(rt)
	}
}

// TODO: cleanup helper
func (b *Bridge) sendReplyToShim(rpcProcType rpcProc, req request) error {
	respType := msgTypeResponse | msgType(rpcProcType)
	var msgBase requestBase
	_ = json.Unmarshal(req.message, &msgBase)
	resp := &responseBase{
		Result: 0, // 0 means succes!
		//	ErrorMessage: "",
		//fmt.Sprintf("Request %v not allowed", req.typ.String()),
		ActivityID: msgBase.ActivityID,
	}
	msgb, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	b.sendMessageToShim(respType, getMessageID(req.header), msgb)

	return nil
}

// TODO (kiashok): Cleanup.
// Sends early reply to shim
func (b *Bridge) sendMessageToShim(typ msgType, id int64, msg []byte) {
	var h [hdrSize]byte
	binary.LittleEndian.PutUint32(h[:], uint32(typ))
	binary.LittleEndian.PutUint32(h[4:], uint32(len(msg)+16))
	binary.LittleEndian.PutUint64(h[8:], uint64(id))

	b.sendToShimCh <- request{
		header:  h,
		message: msg,
	}
	// time.Sleep(2 * time.Second)
}

func (b *Bridge) modifySettings(req *request) error {
	log.Printf("\n rpcModifySettings handler \n")

	//skipSendToGCS := false
	if err := b.unmarshalModifySettingsAndForward(req); err != nil {
		log.Printf("unmarshalModifySettingsAndForward failed: %s\n", err)
		return err
	}

	// If we've reached here, means the policy has allowed it.
	// So forward msg to inbox GCS.
	//	if !skipSendToGCS {
	//		b.forwardMessageToGCS(*req)
	//	}

	return nil
}

func (b *Bridge) negotiateProtocol(req *request) error {
	var r negotiateProtocolRequest
	var err error
	if err = json.Unmarshal(req.message, &r); err != nil {
		return fmt.Errorf("failed to unmarshal rpcNegotiateProtocol: %v", req)
	}
	log.Printf("rpcNegotiateProtocol: negotiateProtocolRequest{ requestBase %v, MinVersion: %v, MaxVersion: %v }", r.requestBase, r.MinimumVersion, r.MaximumVersion)

	// If we've reached here, means the policy has allowed it.
	// So forward msg to inbox GCS.
	b.sendToGCSCh <- *req

	return nil
}

func (b *Bridge) dumpStacks(req *request) error {
	var r dumpStacksRequest
	var err error
	if err = json.Unmarshal(req.message, &r); err != nil {
		return fmt.Errorf("failed to unmarshal rpcStart: %v", req)
	}
	var ctx context.Context
	err = b.PolicyEnforcer.securityPolicyEnforcer.EnforceDumpStacksPolicy(ctx)

	if err != nil {
		return errors.Wrapf(err, "dump stacks denied due to policy")
	}

	log.Printf("rpcDumpStacks: \n requestBase: %v", r.requestBase)

	// If we've reached here, means the policy has allowed it.
	// So forward msg to inbox GCS.
	b.sendToGCSCh <- *req

	return nil
}

func (b *Bridge) deleteContainerState(req *request) error {
	var r deleteContainerStateRequest
	var err error
	if err = json.Unmarshal(req.message, &r); err != nil {
		return fmt.Errorf("failed to unmarshal rpcStart: %v", req)
	}
	log.Printf("rpcDeleteContainerRequest: \n requestBase: %v", r.requestBase)

	// If we've reached here, means the policy has allowed it.
	// So forward msg to inbox GCS.
	b.sendToGCSCh <- *req

	return nil
}

func (b *Bridge) updateContainer(req *request) error {
	// No callers in the code for rpcUpdateContainer

	// If we've reached here, means the policy has allowed it.
	// So forward msg to inbox GCS.
	b.sendToGCSCh <- *req

	return nil
}

func (b *Bridge) lifecycleNotification(req *request) error {
	// No callers in the code for rpcLifecycleNotification

	// If we've reached here, means the policy has allowed it.
	// So forward msg to inbox GCS.
	b.sendToGCSCh <- *req

	return nil
}
