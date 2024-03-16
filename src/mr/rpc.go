package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// example to show how to declare the arguments
// and reply for an RPC.
type JobType int

const (
	Wait JobType = iota
	MapJob
	ReduceJob
	CompleteJob
)

type AskForJobArgs struct {
	EmptyNum int // 暂时不用这个字段，只用来防gob报错
}
type JobDoneArgs struct {
	Id int // 执行完成的reduceOrReduceId
}
type JobDoneReply struct {
	EmptyNum int // 暂时不用这个字段，只用来防gob报错
}

type WorkerProperty struct {
	WorkId        int
	Type          JobType
	MapOrReduceId int // 需要工作的文件编号
	MapFileName   string
	NReduce       int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
