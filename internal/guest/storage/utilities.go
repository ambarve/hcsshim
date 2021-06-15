// +build linux

package storage

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/Microsoft/hcsshim/internal/log"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// export this variable so it can be mocked to aid in testing for consuming packages
var filepathglob = filepath.Glob

// WaitForFileMatchingPattern waits for a single file that matches the given path pattern and returns the full path
// to the resulting file
func WaitForFileMatchingPattern(ctx context.Context, pattern string) (string, error) {
	for {
		files, err := filepathglob(pattern)
		if err != nil {
			return "", err
		}
		if len(files) == 0 {
			select {
			case <-ctx.Done():
				return "", errors.Wrapf(ctx.Err(), "timed out waiting for file matching pattern %s to exist", pattern)
			default:
				time.Sleep(time.Millisecond * 10)
				continue
			}
		} else if len(files) > 1 {
			return "", fmt.Errorf("more than one file could exist for pattern \"%s\"", pattern)
		}
		return files[0], nil
	}
}

// If the unmount call fails with unix.EBUSY error code then there is some process that is
// keeping the unmount target busy (i.e has open handle). When debugging such failures it
// is helpful to identify what that process is. This function logs the output of `fuser
// <unmountTarget>` to get the pids of the processes that are keeping the unmountTarget
// busy and then it logs the output of `ps` so that we can find the processes.
func logUnmountErrBusyDebugLogs(ctx context.Context, unmountTarget string) {
	fuserCmd := exec.Command("fuser", unmountTarget)
	var outBuf, errBuf bytes.Buffer
	fuserCmd.Stdout = &outBuf
	fuserCmd.Stderr = &errBuf
	err := fuserCmd.Run()
	if err != nil {
		log.G(ctx).Errorf("failed to run fuser: %s", err)
		return
	}
	log.G(ctx).WithFields(logrus.Fields{
		"command": strings.Join(fuserCmd.Args, " "),
		"stdout":  outBuf.String(),
		"stderr":  errBuf.String(),
	}).Warnf("unmount %s failure debug logs: fuser", unmountTarget)

	outBuf.Reset()
	errBuf.Reset()
	psCmd := exec.Command("ps", "-o", "pid,user,comm,args")
	psCmd.Stdout = &outBuf
	psCmd.Stderr = &errBuf
	err = psCmd.Run()
	if err != nil {
		log.G(ctx).Errorf("failed to run ps: %s", err)
		return
	}
	log.G(ctx).WithFields(logrus.Fields{
		"command": strings.Join(psCmd.Args, " "),
		"stdout":  outBuf.String(),
		"stderr":  errBuf.String(),
	}).Warnf("unmount %s failure debug logs: ps", unmountTarget)
}
