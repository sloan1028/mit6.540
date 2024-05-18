package shardkv

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
