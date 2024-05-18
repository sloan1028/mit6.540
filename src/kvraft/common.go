package kvraft

import (
	"fmt"
	"log"
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
)

type Err string

const (
	GetOp OpType = iota
	PutOp
	AppendOp
)

type OpType int

type CommandRequest struct {
	Key       string
	Value     string
	Op        OpType
	CommandId int64
	ClerkId   int64
}

type CommandResponse struct {
	Err   Err
	Value string
}

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
	return 0
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
