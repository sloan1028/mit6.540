package shardkv

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
	"bytes"
	"log"
	"sync"
	"time"
)

const (
	HandleOpTimeOut = time.Millisecond * 500
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	cfg         shardctrler.Config
	mck         *shardctrler.Clerk
	lastApplied int // 防止有旧的提交又apply进状态机了

	// Your definitions here.
	stateMachine map[string]string
	Session      map[int64]CommandSession
	notifyChans  map[int]*chan CommandSession

	toOutShards map[int]map[int]map[string]string // ConfigNum、shard 确定一个需要传送的stateMachine块
	comInShards map[int]int                       // Shard->ConfigNum这个应该是用来看新配置下有哪些Shard需要新增，以及他们的ConfigNum号
	myShards    map[int]bool                      // 用来维护查看当前这个Group掌管了哪些Shard
	garbageList map[int]map[int]bool              // ConfigNum、Shard->Group
}

type CommandSession struct {
	LastCommandId int64
	Value         string
	Err           Err
	CommandTerm   int
}

func (kv *ShardKV) Command(args *CommandRequest, reply *CommandResponse) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	shard := key2shard(args.Key)
	kv.mu.Lock()
	Debug(dLock, "GID: %d, ID: %d Command Get Lock1\n", kv.gid, kv.me)
	gid := kv.cfg.Shards[shard]
	kv.mu.Unlock()
	Debug(dLock, "GID: %d, ID: %d Command UnLock1\n", kv.gid, kv.me)

	if kv.gid != gid {
		reply.Err = ErrWrongGroup
		return
	}
	if args.Op != GetOp {
		kv.mu.Lock()
		Debug(dLock, "GID: %d, ID: %d Command Get Lock2\n", kv.gid, kv.me)
		if res, ok := kv.Session[args.ClerkId]; ok {
			if res.LastCommandId == args.CommandId && res.Err == OK {
				reply.Err = OK
				kv.mu.Unlock()
				Debug(dLock, "GID: %d, ID: %d Command UnLock2\n", kv.gid, kv.me)
				return
			}
		}
		kv.mu.Unlock()
		Debug(dLock, "GID: %d, ID: %d Command UnLock2\n", kv.gid, kv.me)
	}
	res := kv.handleOp(*args)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) handleOp(operation CommandRequest) (result CommandSession) {
	index, term, isLeader := kv.rf.Start(operation)
	if !isLeader {
		result.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	Debug(dLock, "GID: %d, ID: %d HandleOp Get Lock1\n", kv.gid, kv.me)
	newCh := make(chan CommandSession)
	kv.notifyChans[index] = &newCh
	kv.mu.Unlock()
	Debug(dLock, "GID: %d, ID: %d HandleOp UnLock1\n", kv.gid, kv.me)

	defer func() {
		kv.mu.Lock()
		Debug(dLock, "GID: %d, ID: %d HandleOp Get Lock2\n", kv.gid, kv.me)
		delete(kv.notifyChans, index)
		close(newCh)
		kv.mu.Unlock()
		Debug(dLock, "GID: %d, ID: %d HandleOp UnLock12\n", kv.gid, kv.me)
	}()

	select {
	case <-time.After(HandleOpTimeOut):
		result.Err = ErrTimeOut
		return
	case msg, success := <-newCh:
		if success && msg.CommandTerm == term {
			result = msg
			return
		} else {
			result.Err = ErrTimeOut
			return
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) ListenApplyCh() {
	for {
		select {
		case applyMsg := <-kv.applyCh:
			if _, ok := applyMsg.Command.(shardctrler.Config); ok {
				kv.applyCfg(&applyMsg)
				kv.checkDoSnapshot()
			} else if _, ok := applyMsg.Command.(MigrateReply); ok {
				kv.applyShard(&applyMsg)
				kv.checkDoSnapshot()
			} else if _, ok := applyMsg.Command.(GcClearArgs); ok {
				kv.applyGarbageCollection(&applyMsg)
				kv.checkDoSnapshot()
			} else {
				if applyMsg.CommandValid {
					kv.parseApplyMsgToCommand(&applyMsg)
					kv.checkDoSnapshot()
				} else if applyMsg.SnapshotValid {
					// 处理快照
					kv.ReadSnapshot(applyMsg.Snapshot)
				} else {
					Debug(dError, "ApplyMsg Type Fault!!!\n")
				}
			}
		}
	}
}

func (kv *ShardKV) parseApplyMsgToCommand(applyMsg *raft.ApplyMsg) {
	kv.mu.Lock()
	Debug(dLock, "GID: %d, ID: %d parseApplyMsgToCommand Get Lock1\n", kv.gid, kv.me)
	defer kv.mu.Unlock()
	defer Debug(dLock, "GID: %d, ID: %d parseApplyMsgToCommand UnLock1\n", kv.gid, kv.me)
	// 可能会有旧的applyMsg进来
	if applyMsg.CommandIndex <= kv.lastApplied {
		return
	}
	kv.lastApplied = applyMsg.CommandIndex
	var response CommandSession
	command, _ := applyMsg.Command.(CommandRequest)
	shard := key2shard(command.Key)
	if _, ok := kv.myShards[shard]; !ok {
		response.Err = ErrWrongGroup
	} else {
		if command.Op != GetOp && kv.isDuplicateRequest(command.ClerkId, command.CommandId) {
			command, _ := kv.Session[command.ClerkId]
			response = command
		} else {
			response = kv.executeStateMachine(&command, applyMsg.Term)
		}
	}

	// only notify related channel for currentTerm's log when node is leader
	if currentTerm, isLeader := kv.rf.GetState(); isLeader && applyMsg.Term == currentTerm {
		response.CommandTerm = applyMsg.Term
		if ch, ok := kv.notifyChans[applyMsg.CommandIndex]; ok {
			select {
			case *ch <- response: // 注意这里使用 *ch 来解引用指针
				// 成功发送
			case <-time.After(time.Millisecond * 100): // 发送超时
				// 超时处理
			}
		}
	}
}

func (kv *ShardKV) isDuplicateRequest(clerkId int64, opId int64) bool {
	if res, ok := kv.Session[clerkId]; ok {
		if res.LastCommandId == opId {
			return true
		}
	}
	return false
}

func (kv *ShardKV) executeStateMachine(operation *CommandRequest, term int) (session CommandSession) {
	session.LastCommandId = operation.CommandId
	session.CommandTerm = term
	session.Err = OK
	switch operation.Op {
	case GetOp:
		session.Value = kv.stateMachine[operation.Key]
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

func (kv *ShardKV) applyShard(applyMsg *raft.ApplyMsg) {
	migrateData := applyMsg.Command.(MigrateReply)
	kv.mu.Lock()
	Debug(dLock, "GID: %d, ID: %d applyShard Get Lock\n", kv.gid, kv.me)
	defer kv.mu.Unlock()
	defer Debug(dLock, "GID: %d, ID: %d applyShard UnLock\n", kv.gid, kv.me)
	if migrateData.ConfigNum != kv.cfg.Num-1 { //我当前需要的migrateData,在发送者那里应该是我当前的cfgNum-1
		return
	}
	kv.lastApplied = applyMsg.CommandIndex
	delete(kv.comInShards, migrateData.Shard)
	if _, ok := kv.myShards[migrateData.Shard]; !ok {
		kv.myShards[migrateData.Shard] = true
		for k, v := range migrateData.DB {
			kv.stateMachine[k] = v
		}
		for client, session := range migrateData.Client2Session {
			if kv.Session[client].LastCommandId <= session.LastCommandId {
				kv.Session[client] = session
			}
		}
		if _, ok := kv.garbageList[migrateData.ConfigNum]; !ok {
			kv.garbageList[migrateData.ConfigNum] = make(map[int]bool)
		}
		kv.garbageList[migrateData.ConfigNum][migrateData.Shard] = true
	}
}

func (kv *ShardKV) applyGarbageCollection(applyMsg *raft.ApplyMsg) {
	gcData := applyMsg.Command.(GcClearArgs)
	kv.mu.Lock()
	Debug(dLock, "GID: %d, ID: %d applyGarbageCollection Get Lock1\n", kv.gid, kv.me)
	defer kv.mu.Unlock()
	defer Debug(dLock, "GID: %d, ID: %d applyGarbageCollection UnLock\n", kv.gid, kv.me)
	kv.lastApplied = applyMsg.CommandIndex
	if _, ok := kv.toOutShards[gcData.ConfigNum]; ok {
		delete(kv.toOutShards[gcData.ConfigNum], gcData.Shard)
		if len(kv.toOutShards[gcData.ConfigNum]) == 0 {
			delete(kv.toOutShards, gcData.ConfigNum)
		}
	}
	response := CommandSession{Err: OK}
	if _, isLeader := kv.rf.GetState(); isLeader {
		if ch, ok := kv.notifyChans[applyMsg.CommandIndex]; ok {
			select {
			case *ch <- response: // 注意这里使用 *ch 来解引用指针
				// 成功发送
			case <-time.After(time.Millisecond * 100): // 发送超时
				// 超时处理
			}
		}
	}
}

// 收到新的Configuration后，判断自己要丢掉的Shard和要接收的Shard
func (kv *ShardKV) applyCfg(applyMsg *raft.ApplyMsg) {
	cfg := applyMsg.Command.(shardctrler.Config)
	kv.mu.Lock()
	Debug(dLock, "GID: %d, ID: %d applyCfg Get Lock1\n", kv.gid, kv.me)
	defer kv.mu.Unlock()
	defer Debug(dLock, "GID: %d, ID: %d applyCfg UnLock1\n", kv.gid, kv.me)
	if cfg.Num <= kv.cfg.Num {
		return
	}
	kv.lastApplied = applyMsg.CommandIndex
	oldCfg, toOutShard := kv.cfg, kv.myShards
	kv.myShards, kv.cfg = make(map[int]bool), cfg
	for shard, gid := range cfg.Shards {
		if gid != kv.gid {
			continue
		}
		if _, ok := toOutShard[shard]; ok || oldCfg.Num == 0 {
			// 新配置里的该shard，在老配置也被当前节点持有 -> 当前的节点可以继续持有这个shard，啥都不用干
			kv.myShards[shard] = true
			delete(toOutShard, shard)
		} else {
			// 新配置里的该shard，在老配置里不持有 -> 需要pull到这个shard Data
			kv.comInShards[shard] = oldCfg.Num
		}
	}
	// 把需要丢掉的Shard拉出来，在主状态机中删掉
	if len(toOutShard) > 0 {
		kv.toOutShards[oldCfg.Num] = make(map[int]map[string]string)
		for shard := range toOutShard {
			//DPrintf("ID: %d toOutShard ConfigNum: %d, shard: %d\n", kv.me, oldCfg.Num, shard)
			outDb := make(map[string]string)
			for k, v := range kv.stateMachine {
				if key2shard(k) == shard {
					outDb[k] = v
					delete(kv.stateMachine, k)
				}
			}
			kv.toOutShards[oldCfg.Num][shard] = outDb
		}
	}
}

func (kv *ShardKV) checkDoSnapshot() {
	kv.mu.Lock()
	if kv.maxraftstate > 0 && kv.rf.Persister.RaftStateSize() >= kv.maxraftstate {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.lastApplied)
		e.Encode(kv.stateMachine)
		e.Encode(kv.Session)
		e.Encode(kv.comInShards)
		e.Encode(kv.toOutShards)
		e.Encode(kv.myShards)
		e.Encode(kv.cfg)
		e.Encode(kv.garbageList)
		lastApplied := kv.lastApplied
		kv.mu.Unlock()
		go kv.rf.Snapshot(lastApplied, w.Bytes())
		return
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) ReadSnapshot(snapshot []byte) {
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
	var comInShards map[int]int
	var toOutShards map[int]map[int]map[string]string
	var myShards map[int]bool
	var garbageList map[int]map[int]bool
	var cfg shardctrler.Config
	if d.Decode(&lastApplied) != nil ||
		d.Decode(&hashTable) != nil ||
		d.Decode(&session) != nil ||
		d.Decode(&comInShards) != nil ||
		d.Decode(&toOutShards) != nil ||
		d.Decode(&myShards) != nil ||
		d.Decode(&cfg) != nil ||
		d.Decode(&garbageList) != nil {
		log.Fatalf("%v Decode error %v\n", d, snapshot)
	} else {
		if lastApplied <= kv.lastApplied {
			//DPrintf("Kvraft ID: %d ReadSnapshot, lastApplied: %d, kv.lastApplied: %d, 大失败！\n", kv.me, lastApplied, kv.lastApplied)
			return
		}
		kv.comInShards = comInShards
		kv.toOutShards = toOutShards
		kv.myShards = myShards
		kv.garbageList = garbageList
		kv.cfg = cfg
		kv.stateMachine = hashTable
		kv.lastApplied = lastApplied
		kv.Session = session
	}
}

func (kv *ShardKV) PollNewCfg() {
	kv.mu.Lock()
	Debug(dLock, "GID: %d, ID: %d PollNewCfg Get Lock1\n", kv.gid, kv.me)
	// 当前更改Configuration还未结束，不要去再次拉新
	if len(kv.comInShards) > 0 {
		kv.mu.Unlock()
		Debug(dLock, "GID: %d, ID: %d PollNewCfg UnLock1\n", kv.gid, kv.me)
		return
	}
	next := kv.cfg.Num + 1
	kv.mu.Unlock()
	Debug(dLock, "GID: %d, ID: %d PollNewCfg UnLock1\n", kv.gid, kv.me)
	cfg := kv.mck.Query(next)
	if cfg.Num == next {
		kv.rf.Start(cfg)
	}
}

func (kv *ShardKV) ShardMigration(args *MigrateArgs, reply *MigrateReply) {
	reply.Shard, reply.ConfigNum = args.Shard, args.ConfigNum
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	Debug(dLock, "GID: %d, ID: %d ShardMigration Get Lock1\n", kv.gid, kv.me)
	defer kv.mu.Unlock()
	defer Debug(dLock, "GID: %d, ID: %d ShardMigration UnLock\n", kv.gid, kv.me)
	reply.Err = ErrWrongGroup
	if args.ConfigNum >= kv.cfg.Num { // 为什么==也要return? 因为需要的args的configNum来自oldConfigNum，如果相等说明本方也落后了
		return
	}
	reply.Err = OK
	reply.DB = make(map[string]string)
	reply.Client2Session = make(map[int64]CommandSession)
	for clientId, session := range kv.Session {
		reply.Client2Session[clientId] = session
	}
	for k, v := range kv.toOutShards[args.ConfigNum][args.Shard] {
		reply.DB[k] = v
	}
}

func (kv *ShardKV) GarbageCollection(args *GcClearArgs, reply *GcClearReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	Debug(dLock, "GID: %d, ID: %d GarbageCollection Get Lock1\n", kv.gid, kv.me)
	if _, ok := kv.toOutShards[args.ConfigNum]; !ok {
		kv.mu.Unlock()
		defer Debug(dLock, "GID: %d, ID: %d GarbageCollection UnLock2\n", kv.gid, kv.me)
		return
	}
	if _, ok := kv.toOutShards[args.ConfigNum][args.Shard]; !ok {
		kv.mu.Unlock()
		defer Debug(dLock, "GID: %d, ID: %d GarbageCollection UnLock3\n", kv.gid, kv.me)
		return
	}
	kv.mu.Unlock()
	defer Debug(dLock, "GID: %d, ID: %d GarbageCollection UnLock4\n", kv.gid, kv.me)
	reply.Err = kv.handleGC(args)
}
func (kv *ShardKV) handleGC(args *GcClearArgs) Err {
	index, term, isLeader := kv.rf.Start(*args)
	if !isLeader {
		return ErrWrongLeader
	}
	kv.mu.Lock()
	Debug(dLock, "GID: %d, ID: %d handleGC Get Lock1\n", kv.gid, kv.me)

	newCh := make(chan CommandSession)
	kv.notifyChans[index] = &newCh
	kv.mu.Unlock()
	Debug(dLock, "GID: %d, ID: %d handleGC UnLock1\n", kv.gid, kv.me)

	defer func() {
		kv.mu.Lock()
		Debug(dLock, "GID: %d, ID: %d handleGC Get Lock2\n", kv.gid, kv.me)
		delete(kv.notifyChans, index)
		close(newCh)
		kv.mu.Unlock()
		Debug(dLock, "GID: %d, ID: %d handleGC UnLock2\n", kv.gid, kv.me)
	}()

	select {
	case <-time.After(HandleOpTimeOut):
		return ErrTimeOut
	case msg, success := <-newCh:
		if success && msg.CommandTerm == term {
			return msg.Err
		} else {
			return ErrTimeOut
		}
	}
}

func (kv *ShardKV) tryPullShard() {
	kv.mu.Lock()
	Debug(dLock, "GID: %d, ID: %d tryPullShard Get Lock1\n", kv.gid, kv.me)
	if len(kv.comInShards) == 0 {
		kv.mu.Unlock()
		Debug(dLock, "GID: %d, ID: %d tryPullShard UnLock1\n", kv.gid, kv.me)
		return
	}
	var wait sync.WaitGroup
	for shard, idx := range kv.comInShards {
		cfg := kv.mck.Query(idx)
		wait.Add(1)
		go func(shard int, cfg shardctrler.Config) {
			defer wait.Done()
			args := MigrateArgs{kv.gid, cfg.Num, shard}
			gid := cfg.Shards[shard]
			for _, server := range cfg.Groups[gid] {
				srv := kv.make_end(server)
				reply := MigrateReply{}
				if ok := srv.Call("ShardKV.ShardMigration", &args, &reply); ok {
					if reply.Err == OK {
						kv.rf.Start(reply)
					}
				}
			}
		}(shard, cfg)
	}
	kv.mu.Unlock()
	wait.Wait()
}

func (kv *ShardKV) tryGcClear() {
	kv.mu.Lock()
	Debug(dLock, "GID: %d, ID: %d tryGcClear Get Lock1\n", kv.gid, kv.me)
	defer Debug(dLock, "GID: %d, ID: %d tryGcClear UnLock1\n", kv.gid, kv.me)
	if len(kv.garbageList) == 0 {
		kv.mu.Unlock()
		return
	}
	var wait sync.WaitGroup
	for cfgNum, shards := range kv.garbageList {
		for shard := range shards {
			cfg := kv.mck.Query(cfgNum)
			wait.Add(1)
			go func(shard int, cfg shardctrler.Config) {
				defer wait.Done()
				args := GcClearArgs{kv.gid, cfg.Num, shard}
				gid := cfg.Shards[shard]
				for _, server := range cfg.Groups[gid] {
					srv := kv.make_end(server)
					reply := GcClearReply{}
					if ok := srv.Call("ShardKV.GarbageCollection", &args, &reply); ok {
						if reply.Err == OK {
							kv.mu.Lock()
							Debug(dLock, "GID: %d, ID: %d tryGcClear Get Lock2\n", kv.gid, kv.me)
							delete(kv.garbageList[cfgNum], shard)
							if len(kv.garbageList[cfgNum]) == 0 {
								delete(kv.garbageList, cfgNum)
							}
							kv.mu.Unlock()
							Debug(dLock, "GID: %d, ID: %d tryGcClear UnLock1\n", kv.gid, kv.me)
						}
					}
				}
			}(shard, cfg)
		}
	}
	kv.mu.Unlock()
	wait.Wait()
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(CommandRequest{})
	labgob.Register(CommandSession{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(MigrateReply{})
	labgob.Register(GcClearArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.stateMachine = make(map[string]string)
	kv.Session = make(map[int64]CommandSession)
	kv.notifyChans = make(map[int]*chan CommandSession)
	kv.toOutShards = make(map[int]map[int]map[string]string)
	kv.comInShards = make(map[int]int)
	kv.myShards = make(map[int]bool)
	kv.garbageList = make(map[int]map[int]bool)

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.ReadSnapshot(persister.ReadSnapshot())
	go kv.ListenApplyCh()
	go kv.Monitor(kv.PollNewCfg, 50*time.Millisecond) // PollNewCfg只做一件事：Leader把新配置信息拉到Raft中进行同步
	go kv.Monitor(kv.tryPullShard, 50*time.Millisecond)
	go kv.Monitor(kv.tryGcClear, 50*time.Millisecond)

	return kv
}

func (kv *ShardKV) Monitor(action func(), timeout time.Duration) {
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(timeout)
	}
}
