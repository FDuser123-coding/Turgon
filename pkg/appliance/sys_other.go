//go:build !linux

package appliance

import "syscall"

func sysProcAttr() *syscall.SysProcAttr { return nil }
