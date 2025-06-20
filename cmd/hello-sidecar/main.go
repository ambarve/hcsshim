//go:build windows
// +build windows

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Microsoft/go-winio"
	"github.com/Microsoft/hcsshim/internal/gcs/prot"
	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/debug"
)

type handler struct {
	fromsvc chan error
}

func (h *handler) Execute(args []string, r <-chan svc.ChangeRequest, status chan<- svc.Status) (bool, uint32) {
	const cmdsAccepted = svc.AcceptStop | svc.AcceptShutdown | svc.Accepted(windows.SERVICE_ACCEPT_PARAMCHANGE)

	status <- svc.Status{State: svc.StartPending, Accepts: 0}
	// unblock runService()
	h.fromsvc <- nil

	status <- svc.Status{State: svc.Running, Accepts: cmdsAccepted}

loop:
	for c := range r {
		switch c.Cmd {
		case svc.Interrogate:
			status <- c.CurrentStatus
		case svc.Stop, svc.Shutdown:
			fmt.Println("Shutting service...!")
			break loop
		case svc.Pause:
			status <- svc.Status{State: svc.Paused, Accepts: cmdsAccepted}
		case svc.Continue:
			status <- svc.Status{State: svc.Running, Accepts: cmdsAccepted}
		default:
			fmt.Printf("Unexpected service control request #%d", c)
		}
	}

	status <- svc.Status{State: svc.StopPending}
	return false, 1
}

func runService(name string, isDebug bool) error {
	h := &handler{
		fromsvc: make(chan error),
	}

	var err error
	go func() {
		if isDebug {
			err = debug.Run(name, h)
			if err != nil {
				fmt.Printf("Error running service in debug mode.Err: %s", err)
			}
		} else {
			err = svc.Run(name, h)
			if err != nil {
				fmt.Printf("Error running service in Service Control mode.Err %s", err)
			}
		}
		h.fromsvc <- err
	}()

	return <-h.fromsvc
}

func runHelloSidecarService() {
	var err error
	ctx := context.Background()

	chsrv := make(chan error)
	go func() {
		defer close(chsrv)
		err = runService("gcs-sidecar", false)
		chsrv <- err
	}()

	r := <-chsrv
	if r != nil {
		fmt.Printf("chsrv failed: %s\n", r)
		return
	}
	fmt.Printf("starting hello sidecar service\n")

	hvsockAddr := &winio.HvsockAddr{
		VMID:      prot.HvGUIDParent,
		ServiceID: prot.WindowsSidecarGcsHvsockServiceID,
	}

	fmt.Printf("attempting to dial hvsock...\n")
	shimCon, err := winio.Dial(ctx, hvsockAddr)
	if err != nil {
		fmt.Printf("failed to dial hvsock: %s\n", err)
		return
	}
	defer shimCon.Close()

	fmt.Printf("connected to server\n")

	for {
		_, err = shimCon.Write([]byte("hello world!"))
		if err != nil {
			fmt.Printf("failed to write to socket: %s\n", err)
			return
		}
		fmt.Printf("sent hello world message\n")
		time.Sleep(1 * time.Second)
	}
}

func runHelloSidecarServer() {
	fmt.Printf("starting hello sidecar server\n")
	l, err := winio.ListenHvsock(&winio.HvsockAddr{
		ServiceID: prot.WindowsSidecarGcsHvsockServiceID,
	})
	if err != nil {
		fmt.Printf("failed to listen on hvsock: %s\n", err)
		return
	}
	defer l.Close()

	fmt.Printf("listening on hvsock, waiting for connection...\n")
	conn, err := l.Accept()
	if err != nil {
		fmt.Printf("failed to accept connection: %s\n", err)
		return
	}
	defer conn.Close()
	fmt.Printf("connection accepted\n")
	buf := make([]byte, 32)
	for {
		nr, err := conn.Read(buf)
		if err != nil {
			fmt.Printf("read failed: %s\n", err)
			return
		}
		fmt.Printf("received: %s\n", string(buf[:nr]))
	}
}

func main() {
	server := false
	if len(os.Args) > 0 {
		for _, arg := range os.Args {
			if strings.EqualFold(arg, "server") {
				server = true
				break
			}
		}
	}

	if server {
		runHelloSidecarServer()
	} else {
		runHelloSidecarService()
	}

}
