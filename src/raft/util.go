package raft

import (
	"fmt"
	"log"
	"time"
)

type logTopic string

var debugStart time.Time
var debugVerbosity int

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

func getVerbosity() int {
	return 1
	/*
		v := os.Getenv("VERBOSE")
		level := 0
		if v != "" {
			var err error
			level, err = strconv.Atoi(v)
			if err != nil {
				log.Fatalf("Invalid verbosity %v", v)
			}
		}
		return level
	*/
}

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		t := time.Since(debugStart).Microseconds()
		t /= 100
		prefix := fmt.Sprintf("%06d %v ", t, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func Min(x, y int) int {
	if x >= y {
		return y
	}
	return x
}
func Max(x, y int) int {
	if x >= y {
		return x
	}
	return y
}
