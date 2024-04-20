package raft

import "log"

// Debugging
const Debug = false
const DebugLockLog = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func LockLog(format string, a ...interface{}) {
	if DebugLockLog {
		log.Printf(format, a...)
	}
}
