package shardctrler

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sort"
	"sync"
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

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	lastApplied int // 防止有旧的提交又apply进状态机了

	Session     map[int64]CommandSession
	notifyChans map[int]*chan CommandSession

	configs []Config // indexed by config num
}

type CommandSession struct {
	LastCommandId int64
	Config        Config
	Err           Err
	CommandTerm   int
}

func (sc *ShardCtrler) Join(args *CommandRequest, reply *CommandResponse) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	res := sc.handleOp(*args)
	reply.Err = res.Err
}

func (sc *ShardCtrler) Leave(args *CommandRequest, reply *CommandResponse) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	res := sc.handleOp(*args)
	reply.Err = res.Err
}

func (sc *ShardCtrler) Move(args *CommandRequest, reply *CommandResponse) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	res := sc.handleOp(*args)
	reply.Err = res.Err
}

func (sc *ShardCtrler) Query(args *CommandRequest, reply *CommandResponse) {
	// Your code here.
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	res := sc.handleOp(*args)
	reply.Err = res.Err
	reply.Config = res.Config
}

func (sc *ShardCtrler) handleOp(command CommandRequest) (result CommandSession) {
	index, term, isLeader := sc.rf.Start(command)
	if !isLeader {
		result.Err = ErrWrongLeader
		return
	}
	//DPrintf("ID: %d, Start command %v\n", sc.me, command)
	sc.mu.Lock()
	newCh := make(chan CommandSession)
	sc.notifyChans[index] = &newCh
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.notifyChans, index)
		close(newCh)
		sc.mu.Unlock()
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

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) ListenApplyCh() {
	for {
		select {
		case applyMsg := <-sc.applyCh:
			if applyMsg.CommandValid {
				//DPrintf("Kvraft: ID: %d, GetCommandApplyMsg, Index: %d\n", sc.me, applyMsg.CommandIndex)
				sc.parseApplyMsgToCommand(&applyMsg)
			} else {
				DPrintf("ApplyMsg Type Fault!!!\n")
			}
		}
	}
}

func (sc *ShardCtrler) parseApplyMsgToCommand(applyMsg *raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// 可能会有旧的applyMsg进来
	if applyMsg.CommandIndex <= sc.lastApplied {
		return
	}
	sc.lastApplied = applyMsg.CommandIndex
	var response CommandSession
	command, _ := applyMsg.Command.(CommandRequest)
	//DPrintf("ID: %d, Receive Command: %v\n", sc.me, command)
	if command.Op != Query && sc.isDuplicateRequest(command.ClientId, command.CommandId) {
		command, _ := sc.Session[command.ClientId]
		response = command
	} else {
		response = sc.executeStateMachine(&command, applyMsg.Term)
	}

	// only notify related channel for currentTerm's log when node is leader
	if currentTerm, isLeader := sc.rf.GetState(); isLeader && applyMsg.Term == currentTerm {
		response.CommandTerm = applyMsg.Term
		if ch := sc.notifyChans[applyMsg.CommandIndex]; ch != nil {
			*ch <- response // 注意这里使用 *ch 来解引用指针
		} else {
			DPrintf("-----Channel is nil at index: %d\n", applyMsg.CommandIndex)
		}
	}
}

func (sc *ShardCtrler) isDuplicateRequest(clientId int64, opId int64) bool {
	if res, ok := sc.Session[clientId]; ok {
		if res.LastCommandId == opId {
			return true
		}
	}
	return false
}

func (sc *ShardCtrler) executeStateMachine(operation *CommandRequest, term int) (session CommandSession) {
	session.LastCommandId = operation.CommandId
	session.CommandTerm = term
	session.Err = OK
	switch operation.Op {
	case Join:
		sc.HandleJoin(operation.Servers)
		break
	case Leave:
		DPrintf("HandleLeave")
		sc.HandleLeave(operation.GIDs)
		break
	case Move:
		DPrintf("HandleMove")
		sc.HandleMove(operation.Shard, operation.GID)
		break
	case Query:
		session.Config = sc.HandleQuery(operation.Num)
		break
	}
	return
}

func (sc *ShardCtrler) HandleJoin(groups map[int][]string) {
	DPrintf("ID: %d HandleJoin %v\n", sc.me, groups)
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{len(sc.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			DPrintf("ID: %d, gid: %d, newServers: %v\n", sc.me, gid, newServers)
			newConfig.Groups[gid] = newServers
		}
	}
	g2s := Group2Shards(newConfig)
	DPrintf("ID: %d g2s :%v", sc.me, g2s)
	for {
		maxGid, minGid := GetGIDWithMaximumShards(g2s), GetGIDWithMinimumShards(g2s)
		if maxGid != 0 && len(g2s[maxGid])-len(g2s[minGid]) <= 1 {
			break
		}
		g2s[minGid] = append(g2s[minGid], g2s[maxGid][0])
		g2s[maxGid] = g2s[maxGid][1:]
	}
	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	DPrintf("ID: %d, newShards: %v\n", sc.me, newShards)
	newConfig.Shards = newShards
	sc.configs = append(sc.configs, newConfig)
	return
}

func (sc *ShardCtrler) HandleLeave(gids []int) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{len(sc.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
	g2s := Group2Shards(newConfig)
	orphanShards := make([]int, 0)
	for _, gid := range gids {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := g2s[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(g2s, gid)
		}
	}
	var newShards [NShards]int
	// load balancing is performed only when raft groups exist
	if len(newConfig.Groups) != 0 {
		for _, shard := range orphanShards {
			minGid := GetGIDWithMinimumShards(g2s)
			g2s[minGid] = append(g2s[minGid], shard)
		}
		for gid, shards := range g2s {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	newConfig.Shards = newShards
	sc.configs = append(sc.configs, newConfig)
	return
}

func (sc *ShardCtrler) HandleQuery(num int) Config {
	//DPrintf("ID: %d HandleQuery\n", sc.me)
	if num == -1 || num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[num]
}

func (sc *ShardCtrler) HandleMove(shard int, gid int) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{len(sc.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
	var newShards [NShards]int
	newConfig.Shards = newShards
	for index, _ := range newConfig.Shards {
		newConfig.Shards[index] = lastConfig.Shards[index]
	}
	newConfig.Shards[shard] = gid
	sc.configs = append(sc.configs, newConfig)
	return
}

func deepCopy(ori map[int][]string) map[int][]string {
	res := make(map[int][]string)
	for k, v := range ori {
		res[k] = v
	}
	return res
}

func Group2Shards(config Config) map[int][]int {
	res := make(map[int][]int)
	// 注意下这里要先把所有的gid先创建出来
	for gid, _ := range config.Groups {
		res[gid] = make([]int, 0)
	}
	for shard, group := range config.Shards {
		res[group] = append(res[group], shard)
	}
	return res
}

func GetGIDWithMaximumShards(s2g map[int][]int) int {
	// always choose gid 0 if there is any
	if shards, ok := s2g[0]; ok && len(shards) > 0 {
		return 0
	}
	// make iteration deterministic
	var keys []int
	for k := range s2g {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find GID with maximum shards
	index, max := -1, -1
	for _, gid := range keys {
		if len(s2g[gid]) > max {
			index, max = gid, len(s2g[gid])
		}
	}
	return index
}

func GetGIDWithMinimumShards(s2g map[int][]int) int {
	// make iteration deterministic
	var keys []int
	for k := range s2g {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find GID with minimum shards
	index, min := -1, NShards+1
	for _, gid := range keys {
		if gid != 0 && len(s2g[gid]) < min {
			index, min = gid, len(s2g[gid])
		}
	}
	return index
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(CommandRequest{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.Session = make(map[int64]CommandSession)
	sc.notifyChans = make(map[int]*chan CommandSession)
	go sc.ListenApplyCh()

	return sc
}
