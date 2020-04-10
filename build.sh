#!/bin/bash

# CDPx treats any output to stderr as a warning, so redirect output from
# "set -x" to stdout instead of stderr.
BASH_XTRACEFD=1
set -eux

export GOOS=windows
export GOPROXY=off         # Prohibit downloads
export GOFLAGS=-mod=vendor # Build using vendor directory

mkdir -p /source/cdpx-artifacts
cd /source/cdpx-artifacts

go build github.com/Microsoft/hcsshim/cmd/containerd-shim-runhcs-v1
go build github.com/Microsoft/hcsshim/cmd/runhcs
go build github.com/Microsoft/hcsshim/cmd/shimdiag
go build github.com/Microsoft/hcsshim/cmd/tar2ext4
GOOS=linux go build -buildmode=pie github.com/Microsoft/hcsshim/cmd/tar2ext4
go build github.com/Microsoft/hcsshim/internal/tools/zapdir
go build github.com/Microsoft/hcsshim/internal/tools/grantvmgroupaccess

cd /source/test
go test -c github.com/Microsoft/hcsshim/test/cri-containerd --tags functional
mv ./cri-containerd.test.exe /source/cdpx-artifacts
