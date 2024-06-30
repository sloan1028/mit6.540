package shardkv

import (
	"fmt"
	"log"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
)

type Err string

const (
	GetOp Operation = iota
	PutOp
	AppendOp
)

type Operation int

type CommandRequest struct {
	Key       string
	Value     string
	Op        Operation
	CommandId int64
	ClerkId   int64
}

type CommandResponse struct {
	Err   Err
	Value string
}

type MigrateArgs struct {
	Gid       int
	ConfigNum int
	Shard     int
}

type MigrateReply struct {
	Err            Err
	ConfigNum      int
	Shard          int
	DB             map[string]string
	Client2Session map[int64]CommandSession
}

type GcClearArgs struct {
	Gid       int
	ConfigNum int
	Shard     int
}

type GcClearReply struct {
	Err Err
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
	dLock    logTopic = "LOCK"
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
