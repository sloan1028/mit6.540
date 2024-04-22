package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false
const (
	HandleOpTimeOut = time.Millisecond * 500
)

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
	stateMachine map[string]string
	Session      map[int64]CommandSession
	notifyChans  map[int]*chan CommandSession
}

type CommandSession struct {
	LastCommandId int
	Value         string
	Err           Err
	CommandTerm   int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	option := Op{
		ClerkId: args.ClerkId,
		OpId:    args.CommandId,
		Type:    GetOp,
		Key:     args.Key,
	}
	res := kv.handleOp(option)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if res, ok := kv.Session[args.ClerkId]; ok {
		if res.LastCommandId == args.CommandId && res.Err == OK {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	option := Op{
		ClerkId: args.ClerkId,
		OpId:    args.CommandId,
		Type:    PutOp,
		Key:     args.Key,
		Value:   args.Value,
	}
	res := kv.handleOp(option)
	reply.Err = res.Err
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if res, ok := kv.Session[args.ClerkId]; ok {
		if res.LastCommandId == args.CommandId && res.Err == OK {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	option := Op{
		ClerkId: args.ClerkId,
		OpId:    args.CommandId,
		Type:    AppendOp,
		Key:     args.Key,
		Value:   args.Value,
	}
	res := kv.handleOp(option)
	reply.Err = res.Err
}

func (kv *KVServer) handleOp(operation Op) (result CommandSession) {
	//DPrintf("%d 准备添加ClerckId: %v, OpId: %v 到raft里\n", kv.me, operation.ClerkId, operation.OpId)
	index, term, isLeader := kv.rf.Start(operation)
	if !isLeader {
		result.Err = ErrWrongLeader
		return
	}
	//tt := time.Now()
	kv.mu.Lock()
	newCh := make(chan CommandSession)
	kv.notifyChans[index] = &newCh
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.notifyChans, index)
		close(newCh)
		kv.mu.Unlock()
		//log.Printf("time: %v", time.Since(tt))
	}()

	select {
	case <-time.After(HandleOpTimeOut):
		result.Err = ErrTimeOut
		return
	case msg, success := <-newCh:
		//DPrintf("%d 接收到ClerckId: %v, OpId: %v, Index: %v,"+
		//"result: %v, commandTerm: %d, term: %d\n", kv.me, operation.ClerkId, operation.OpId, index, msg.Err, msg.CommandTerm, term)
		if success && msg.CommandTerm == term {
			result = msg
			return
		} else {
			result.Err = ErrTimeOut
			return
		}
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
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				DPrintf("Kvraft: ID: %d, GetCommandApplyMsg, Index: %d\n", kv.me, applyMsg.CommandIndex)
				kv.parseApplyMsgToCommand(&applyMsg)
				kv.checkDoSnapshot() // 检查是否需要执行快照
			} else if applyMsg.SnapshotValid {
				DPrintf("Kvraft: ID: %d, GetSnapshotApplyMsg, Index: %d\n", kv.me, applyMsg.SnapshotIndex)
				// 处理快照
				kv.ReadSnapshot(applyMsg.Snapshot)
			} else {
				DPrintf("ApplyMsg Type Fault!!!\n")
			}
		}
	}
}

func (kv *KVServer) parseApplyMsgToCommand(applyMsg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 可能会有旧的applyMsg进来
	if applyMsg.CommandIndex <= kv.lastApplied {
		return
	}
	kv.lastApplied = applyMsg.CommandIndex
	var response CommandSession
	command, _ := applyMsg.Command.(Op)
	if command.Type == AppendOp {
		DPrintf("Id: %d ParseApplyMsgToCommandIndex: %d clerkId: %v, opId: %v, key: %v, value: %v\n",
			kv.me, applyMsg.CommandIndex, command.ClerkId, command.OpId, command.Key, command.Value)
	}
	if command.Type != GetOp && kv.isDuplicateRequest(command.ClerkId, command.OpId) {
		command, _ := kv.Session[command.ClerkId]
		response = command
		DPrintf("isDuplicateRequest")
	} else {
		response = kv.executeStateMachine(&command, applyMsg.Term)
	}

	// only notify related channel for currentTerm's log when node is leader
	if currentTerm, isLeader := kv.rf.GetState(); isLeader && applyMsg.Term == currentTerm {
		response.CommandTerm = applyMsg.Term
		//DPrintf("%d 准备返回一条response给commandIndex: %d\n", kv.me, applyMsg.CommandIndex)
		if ch := kv.notifyChans[applyMsg.CommandIndex]; ch != nil {
			//DPrintf("%d Apply response:Term: %v commandId: %v err: %v to ch\n",
			//kv.me, response.CommandTerm, response.LastCommandId, response.Err)
			*ch <- response // 注意这里使用 *ch 来解引用指针
		} else {
			// 可能需要处理 nil 指针的情况
			DPrintf("-----Channel is nil at index: %d\n", applyMsg.CommandIndex)
		}
	}
}

func (kv *KVServer) isDuplicateRequest(clerkId int64, opId int) bool {
	if res, ok := kv.Session[clerkId]; ok {
		if res.LastCommandId == opId {
			return true
		}
	}
	return false
}

func (kv *KVServer) executeStateMachine(operation *Op, term int) (session CommandSession) {
	session.LastCommandId = operation.OpId
	session.CommandTerm = term
	session.Err = OK
	switch operation.Type {
	case GetOp:
		session.Value = kv.stateMachine[operation.Key]
		//kv.Session.Store(operation.ClerkId, session)
		break
	case PutOp:
		kv.stateMachine[operation.Key] = operation.Value
		session.Value = operation.Value
		kv.Session[operation.ClerkId] = session
		break
	case AppendOp:
		kv.stateMachine[operation.Key] += operation.Value
		session.Value = kv.stateMachine[operation.Key]
		kv.Session[operation.ClerkId] = session
		break
	}
	return
}

func (kv *KVServer) checkDoSnapshot() {
	kv.mu.Lock()
	if kv.maxraftstate > 0 && kv.rf.Persister.RaftStateSize() >= kv.maxraftstate {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.lastApplied)
		e.Encode(kv.stateMachine)
		e.Encode(kv.Session)
		lastApplied := kv.lastApplied
		kv.mu.Unlock()
		go kv.rf.Snapshot(lastApplied, w.Bytes())
		return
	}
	kv.mu.Unlock()
}

func (kv *KVServer) ReadSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastApplied int
	var hashTable map[string]string
	var session map[int64]CommandSession
	if d.Decode(&lastApplied) != nil || d.Decode(&hashTable) != nil || d.Decode(&session) != nil {
		log.Fatalf("%v Decode error %v\n", d, snapshot)
	} else {
		if lastApplied <= kv.lastApplied {
			DPrintf("Kvraft ID: %d ReadSnapshot, lastApplied: %d, kv.lastApplied: %d, 大失败！\n", kv.me, lastApplied, kv.lastApplied)
			return
		}
		kv.stateMachine = hashTable
		kv.lastApplied = lastApplied
		kv.Session = session
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
	labgob.Register(CommandSession{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.stateMachine = make(map[string]string)
	kv.Session = make(map[int64]CommandSession)
	kv.notifyChans = make(map[int]*chan CommandSession)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.ReadSnapshot(persister.ReadSnapshot())
	// You may need initialization code here.
	go kv.ListenApplyCh()

	return kv
}
