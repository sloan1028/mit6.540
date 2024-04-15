package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false
const TimeOut = 1000

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType int

const (
	GetOp OpType = iota
	PutOp
	AppendOp
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type    OpType
	OpId    int
	ClerkId int64
	Key     string
	Value   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	lastApplied  int // 防止有旧的提交又apply进状态机了

	// Your definitions here.
	hashTable map[string]string
	Session   sync.Map
}

type CommandSession struct {
	LastCommandId int
	Value         string
	Err           Err
}

func (kv *KVServer) getSessionResult(clerkId int64) (CommandSession, bool) {
	if session, ok := kv.Session.Load(clerkId); ok {
		return session.(CommandSession), true
	}
	return CommandSession{}, false
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if res, ok := kv.getSessionResult(args.ClerkId); ok {
		if res.LastCommandId == args.CommandId && res.Err == OK {
			reply.Err = OK
			reply.Value = res.Value
			return
		}
	}

	option := Op{
		ClerkId: args.ClerkId,
		OpId:    args.CommandId,
		Type:    GetOp,
		Key:     args.Key,
	}
	_, _, isLeader := kv.rf.Start(option)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//todo 确认等待这个applyCh已经被提交
	start := time.Now()
	for {
		if time.Since(start) >= TimeOut*time.Millisecond {
			reply.Err = ErrTimeOut
			return
		}
		if res, ok := kv.getSessionResult(args.ClerkId); ok {
			if res.LastCommandId == args.CommandId && res.Err == OK {
				reply.Err = OK
				reply.Value = res.Value
				return
			}
		}
		// 这里不要Sleep太久,否则过不了速度测试,取10ms间隔即可。
		time.Sleep(time.Millisecond * 10)
	}
	reply.Err = OK
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if res, ok := kv.getSessionResult(args.ClerkId); ok {
		if res.LastCommandId == args.CommandId && res.Err == OK {
			reply.Err = OK
			return
		}
	}
	option := Op{
		ClerkId: args.ClerkId,
		OpId:    args.CommandId,
		Type:    PutOp,
		Key:     args.Key,
		Value:   args.Value,
	}
	_, _, isLeader := kv.rf.Start(option)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	start := time.Now()
	for {
		if time.Since(start) >= TimeOut*time.Millisecond {
			reply.Err = ErrTimeOut
			return
		}
		if res, ok := kv.getSessionResult(args.ClerkId); ok {
			if res.LastCommandId == args.CommandId && res.Err == OK {
				reply.Err = OK
				return
			}
		}
		// 这里不要Sleep太久,否则过不了速度测试,取10ms间隔即可。
		time.Sleep(time.Millisecond * 5)
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if res, ok := kv.getSessionResult(args.ClerkId); ok {
		if res.LastCommandId == args.CommandId && res.Err == OK {
			reply.Err = OK
			return
		}
	}
	option := Op{
		ClerkId: args.ClerkId,
		OpId:    args.CommandId,
		Type:    AppendOp,
		Key:     args.Key,
		Value:   args.Value,
	}
	_, _, isLeader := kv.rf.Start(option)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	start := time.Now()
	for {
		if time.Since(start) >= TimeOut*time.Millisecond {
			reply.Err = ErrTimeOut
			return
		}
		if res, ok := kv.getSessionResult(args.ClerkId); ok {
			if res.LastCommandId == args.CommandId && res.Err == OK {
				reply.Err = OK
				DPrintf("timeDur: %v\n", time.Since(start))
				return
			}
		}
		// 这里不要Sleep太久,否则过不了速度测试,取10ms间隔即可。
		time.Sleep(time.Millisecond * 5)
	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) ListenApplyCh() {
	for applyMsg := range kv.applyCh {
		kv.DoApplyCh(&applyMsg)
	}
}
func (kv *KVServer) DoApplyCh(applyMsg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 可能会有旧的applyMsg进来
	if applyMsg.CommandIndex <= kv.lastApplied {
		return
	}
	kv.lastApplied++
	command, _ := applyMsg.Command.(Op)

	// 重复的或是已有的，直接返回。
	if res, ok := kv.getSessionResult(command.ClerkId); ok {
		if res.LastCommandId >= command.OpId {
			return
		}
	}

	if applyMsg.CommandValid {
		switch command.Type {
		case GetOp:
			// 处理 Get 操作
			session := CommandSession{
				Err:           OK,
				Value:         kv.hashTable[command.Key],
				LastCommandId: command.OpId,
			}
			kv.Session.Store(command.ClerkId, session)
			break
		case PutOp:
			// 处理 Put 操作
			kv.hashTable[command.Key] = command.Value
			session := CommandSession{
				Err:           OK,
				Value:         command.Value,
				LastCommandId: command.OpId,
			}
			kv.Session.Store(command.ClerkId, session)
			break
		case AppendOp:
			kv.hashTable[command.Key] += command.Value
			session := CommandSession{
				Err:           OK,
				Value:         kv.hashTable[command.Key],
				LastCommandId: command.OpId,
			}
			kv.Session.Store(command.ClerkId, session)
			break
		default:
			// 其他类型的处理
		}

	} else {
		// 处理其他类型的 Raft 消息，比如快照等
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.hashTable = make(map[string]string)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.ListenApplyCh()

	return kv
}
