package appliance

import "syscall"

// Workers die with the supervisor, even if it is killed outright (systemd
// would stop them too, with KillMode=control-group).
func sysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
}
