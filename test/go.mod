module github.com/Microsoft/hcsshim/test

go 1.16

require (
	github.com/Microsoft/go-winio v0.5.2
	github.com/Microsoft/hcsshim v0.9.1
	github.com/containerd/containerd v1.5.9
	github.com/containerd/containerd/api v1.6.0-beta.3
	github.com/containerd/go-runc v1.0.0
	github.com/containerd/ttrpc v1.1.0
	github.com/containerd/typeurl v1.0.2
	github.com/gogo/protobuf v1.3.2
	github.com/opencontainers/go-digest v1.0.0
	github.com/opencontainers/image-spec v1.0.2
	github.com/opencontainers/runtime-spec v1.0.3-0.20210326190908-1c3f411f0417
	github.com/opencontainers/runtime-tools v0.0.0-20181011054405-1d69bd0f9c39
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20211216021012-1d35b9e2eb4e
	google.golang.org/grpc v1.43.0
	k8s.io/cri-api v0.23.0-alpha.4
)

replace (
	github.com/Microsoft/hcsshim => ../
	github.com/containerd/containerd => github.com/ambarve/containerd v1.5.1-0.20220401234948-5d9ff4a8721f
	google.golang.org/genproto => google.golang.org/genproto v0.0.0-20200224152610-e50cd9704f63
)
