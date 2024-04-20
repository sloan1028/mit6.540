package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	log2 "log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type StateType int

const (
	Follower StateType = iota
	Candidate
	Leader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Term         int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	ApplyMsg ApplyMsg
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	Persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCond   *sync.Cond
	currentTerm int
	votedFor    int
	log         []Log // len(log)代表的是下一个进入log的index，此时获得的这个值上的log是空的

	commitIndex int
	lastApplied int

	// Reinitialized after election
	nextIndex  []int
	matchIndex []int

	stateMu     sync.Mutex
	state       StateType
	delayTimeMu sync.Mutex
	delayTime   int64

	applyCh chan ApplyMsg

	heartTimer *time.Timer
}

func (rf *Raft) ResetHeartTimer(timeStamp int) {
	rf.heartTimer.Reset(time.Duration(timeStamp) * time.Millisecond)
}

func (rf *Raft) GetStateType() (state StateType) {
	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()
	state = rf.state
	return state
}

func (rf *Raft) SetStateType(t StateType) {
	rf.stateMu.Lock()
	defer rf.stateMu.Unlock()
	rf.state = t
}

func (rf *Raft) GetDelayTime() (t int64) {
	rf.delayTimeMu.Lock()
	defer rf.delayTimeMu.Unlock()
	t = rf.delayTime
	return
}

func (rf *Raft) ClearDelayTime() {
	rf.delayTimeMu.Lock()
	defer rf.delayTimeMu.Unlock()
	rf.delayTime = 0
}

func (rf *Raft) AddDelayTime(t int64) {
	rf.delayTimeMu.Lock()
	defer rf.delayTimeMu.Unlock()
	rf.delayTime += t
}

// GetLogOffset 获得切片和实际LogIndex的偏差值，snapShot日志压缩后切片Index和实际LogIndex会有偏差
// Lab只会有一个snapShot在首位，所以没有问题
// 而且这样索引到的速度是O(1)
func (rf *Raft) getLogOffset() int {
	if len(rf.log) > 0 {
		if rf.log[0].ApplyMsg.CommandValid {
			return rf.log[0].ApplyMsg.CommandIndex
		}
		if rf.log[0].ApplyMsg.SnapshotValid {
			return rf.log[0].ApplyMsg.SnapshotIndex
		}
	}
	log2.Fatalf("%d getLogOffset error\n", rf.me)
	return -10086
}

func (rf *Raft) getLastLogIndex() int {
	log := rf.log[len(rf.log)-1]
	if log.ApplyMsg.CommandValid {
		return log.ApplyMsg.CommandIndex
	}
	if log.ApplyMsg.SnapshotValid {
		return log.ApplyMsg.SnapshotIndex
	}
	log2.Fatalf("%d getLastLogIndex error\n", rf.me)
	return -10086
}

func (rf *Raft) getLastLogTerm() int {
	log := rf.log[len(rf.log)-1]
	if log.ApplyMsg.CommandValid {
		return log.ApplyMsg.Term
	}
	if log.ApplyMsg.SnapshotValid {
		return log.ApplyMsg.SnapshotTerm
	}
	log2.Fatalf("%d getLastLogTerm error\n", rf.me)
	return -10086
}

func (rf *Raft) getLogTerm(logIndex int) int {
	offset := rf.getLogOffset()
	if logIndex-offset >= len(rf.log) {
		//log2.Printf("----------%d getLogTerm Error!! logIndex: %d, offset: %d, logLen: %d\n", rf.me, logIndex, offset, len(rf.log))
		return -1
	}
	if logIndex-offset > 0 {
		return rf.log[logIndex-offset].ApplyMsg.Term
	} else if logIndex-offset == 0 {
		if rf.log[0].ApplyMsg.SnapshotValid {
			return rf.log[0].ApplyMsg.SnapshotTerm
		}
		return rf.log[0].ApplyMsg.Term
	}
	//log2.Fatalf("----------%d getLogTerm Error!! logIndex: %d, offset: %d, logLen: %d\n", rf.me, logIndex, offset, len(rf.log))
	return -1
}

func (rf *Raft) getLog(logIndex int) Log {
	offset := rf.getLogOffset()
	return rf.log[logIndex-offset]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	LockLog("%d GetState GetLock\n", rf.me)
	defer LockLog("%d GetState UnLock\n", rf.me)
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.GetStateType() == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	saveLog := rf.log[1:]
	e.Encode(saveLog)
	e.Encode(rf.log[0].ApplyMsg.SnapshotTerm)
	e.Encode(rf.log[0].ApplyMsg.SnapshotIndex)
	raftState := w.Bytes()

	//DPrintf("rf: %d, Save State: SnapshotTerm %d, SnapshotIndex %d snapshotSize %d",
	//rf.me, rf.log[0].ApplyMsg.SnapshotTerm, rf.log[0].ApplyMsg.SnapshotIndex, len(rf.log[0].ApplyMsg.Snapshot))
	rf.Persister.Save(raftState, rf.log[0].ApplyMsg.Snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var curTerm int
	var voteFor int
	var log []Log
	var SnapshotTerm int
	var SnapshotIndex int
	if d.Decode(&curTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&SnapshotTerm) != nil || d.Decode(&SnapshotIndex) != nil {
		//DPrintf("%v Decode error %v\n", d, data)
	} else {
		rf.currentTerm = curTerm
		rf.votedFor = voteFor
		rf.log = append(rf.log, log...)
		if SnapshotIndex != 0 {
			rf.log[0].ApplyMsg.CommandValid = false
			rf.log[0].ApplyMsg.SnapshotValid = true
			rf.log[0].ApplyMsg.SnapshotIndex = SnapshotIndex
			rf.log[0].ApplyMsg.SnapshotTerm = SnapshotTerm
			// 这里重开时要把快照拉回来，否则下次再存的时候，这里是空的直接把快照弄没了。Bug From 3D最后一个TestSnapshotInit3D
			rf.log[0].ApplyMsg.Snapshot = rf.Persister.ReadSnapshot()
			rf.lastApplied = SnapshotIndex
			//DPrintf("id: %d ReadPersist, ApplyMsg LastIndex: %d\n", rf.me, rf.log[0].ApplyMsg.SnapshotIndex)
			rf.commitIndex = SnapshotIndex

			// 不要瞎并发，会产生乱序的问题，自己给自己挖坑跳
			// 比如这里发snapshot进去后会重置lastApplied, 如果这之前发生提交会被重置掉，而另一个协程不知情的情况下继续发送就会跳过中间的提交
			// but 不并发会产生阻塞，其实这里可以直接交给服务器自己初始化的时候做读取而不用管道
			//go func() {
			//rf.applyCh <- rf.log[0].ApplyMsg
			//}()
		}
		//DPrintf("%v Decode success snapshotTerm: %d snapshotIndex: %d"+
		//"LastApplied: %d\n", rf.me, SnapshotTerm, SnapshotIndex, rf.lastApplied)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock() //为什么我在这里加一个锁，就会导致程序阻塞呢
	LockLog("%d Snapshot GetLock\n", rf.me)
	defer LockLog("%d Snapshot UnLock\n", rf.me)
	defer rf.mu.Unlock()
	offset := rf.getLogOffset()
	if index-offset <= 0 || index-offset >= len(rf.log) {
		return
	}
	term := rf.log[index-offset].ApplyMsg.Term
	newLog := make([]Log, 1)
	newLog[0] = Log{
		ApplyMsg: ApplyMsg{
			SnapshotValid: true,
			SnapshotIndex: index,
			SnapshotTerm:  term,
			Snapshot:      snapshot,
		}}
	idx := 1
	for idx < len(rf.log) {
		applyMsg := rf.log[idx].ApplyMsg
		if applyMsg.CommandValid && applyMsg.CommandIndex <= index {
			idx++
		} else if applyMsg.SnapshotValid && applyMsg.SnapshotIndex <= index {
			idx++
		} else {
			break
		}
	}
	stillNeedLogs := rf.log[idx:]
	newLog = append(newLog, stillNeedLogs...)
	rf.log = newLog
	//log2.Printf("----- %d Do SnapShot index: %d, LogLen to %d, lastIndex: %d, lastTerm: %d\n",
	//rf.me, index, len(rf.log), index, term)
	rf.persist()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int //本lab中不需要用
	Data              []byte
	Done              bool //本lab中不需要用
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	LockLog("%d InstallSnapshot GetLock\n", rf.me)
	defer LockLog("%d InstallSnapshot Unock\n", rf.me)
	defer rf.mu.Unlock()
	//DPrintf("%d Receive InstallSnapshot From %d, LastIndex: %d LastTerm: %d\n",
	//rf.me, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
	reply.Term = rf.currentTerm
	// 1.Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if rf.GetStateType() != Follower && args.Term >= rf.currentTerm {
		rf.SetStateType(Follower)
		rf.votedFor = -1
		rf.persist()
	}
	rf.currentTerm = args.Term
	rf.ClearDelayTime() // 清空delayTime
	// 注意这个清空在确认对方是Leader后就要清空
	// 因为下面第二步如果有很多日志不一致，会花很多时间来同步信息，会把领导选举卡到超时

	// 2. Save snapshot file, discard any existing or partial snapshot with a smaller index
	if rf.log[0].ApplyMsg.CommandValid ||
		(rf.log[0].ApplyMsg.SnapshotValid && rf.log[0].ApplyMsg.SnapshotIndex < args.LastIncludedIndex) {
		newSnapshot := ApplyMsg{
			SnapshotValid: true,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
			Snapshot:      args.Data,
		}
		newApplyMsg := Log{
			ApplyMsg: newSnapshot,
		}
		rf.log[0] = newApplyMsg
		rf.persist()
	}
	// 3. If existing log entry has same index and term as snapshot’s last included entry,
	// retain log entries following it and reply
	for i := 1; i < len(rf.log); i++ {
		log := rf.log[i]
		if log.ApplyMsg.CommandIndex == args.LastIncludedIndex && log.ApplyMsg.Term == args.LastIncludedTerm {
			rf.lastApplied = args.LastIncludedIndex
			rf.commitIndex = args.LastIncludedIndex
			// 也要applyCh
			rf.applyCh <- rf.log[0].ApplyMsg

			saveLog := rf.log[i+1:]
			rf.log = rf.log[:1]
			rf.log = append(rf.log, saveLog...)

			//DPrintf("retain log entries size: %d , lastIndex: %d, lastTerm: %d, rf.commitIndex: %d"+
			//", args.LstIndecludeIndex: %d"+
			//"and return back\n",
			//len(saveLog), rf.getLastLogIndex(), rf.getLastLogTerm(), rf.commitIndex, args.LastIncludedIndex)
			rf.persist()
			return
		}
	}
	// 4. Discard the entire log
	rf.log = rf.log[:1]
	rf.persist()

	// 5.Reset state machine using snapshot contents
	//DPrintf("Reset state machine %d , lastIndex: %d, lastTerm: %d, and return back\n",
	//len(rf.log), rf.getLastLogIndex(), rf.getLastLogTerm())
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	//DPrintf("%d 5.Install Snapshot, Get LastApplied: %d\n", rf.me, args.LastIncludedIndex)
	apm := ApplyMsg{
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
		Snapshot:      args.Data,
	}
	DPrintf("rf %d Apply snapshot size: %d\n", rf.me, len(args.Data))
	DPrintf("id: %d InstallSnapshot, ApplyMsg LastIndex: %d\n", rf.me, rf.log[0].ApplyMsg.SnapshotIndex)
	rf.applyCh <- apm
	rf.persist()
}

func (rf *Raft) GoSendInstallSnapshot(peer int) {
	if rf.GetStateType() != Leader {
		return
	}
	rf.mu.Lock()
	LockLog("%d GoSendInstallSnapshot GetLock\n", rf.me)
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log[0].ApplyMsg.SnapshotIndex,
		LastIncludedTerm:  rf.log[0].ApplyMsg.SnapshotTerm,
		Data:              rf.Persister.ReadSnapshot(),
	}
	applySnapshotIndex := rf.log[0].ApplyMsg.SnapshotIndex
	//DPrintf("%d GoSendInstallSnapshot: to %d\n", rf.me, peer)
	rf.mu.Unlock()
	LockLog("%d GoSendInstallSnapshot UnLock\n", rf.me)
	reply := InstallSnapshotReply{}
	if rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply) {
		rf.mu.Lock()
		LockLog("%d GoSendInstallSnapshot Reply GetLock\n", rf.me)
		rf.nextIndex[peer] = applySnapshotIndex + 1
		rf.matchIndex[peer] = applySnapshotIndex
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.SetStateType(Follower)
			rf.votedFor = -1
			rf.persist()
		}
		rf.mu.Unlock()
		LockLog("%d GoSendInstallSnapshot Reply UnLock\n", rf.me)
	}
	return

}

// 注意一下参数属性都要大写
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//DPrintf("%d Reveive RequestVote From %d, Term %d\n", rf.me, args.CandidateId, args.Term)
	// Your code here (3A, 3B).
	rf.mu.Lock()
	LockLog("%d RequestVote GetLock\n", rf.me)
	defer LockLog("%d RequestVote unLock\n", rf.me)
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		//DPrintf("%d Reveive RequestVote From %d, curTerm: %d,args.Term: %d return false\n",
		//rf.me, args.CandidateId, rf.currentTerm, args.Term)
		return
	}
	if args.Term > rf.currentTerm { // 如果服务器发现自己过时了
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.SetStateType(Follower)
		rf.persist()
	}
	// 接下来检验请求的Candidate能不能通过
	// 检查自己投过票了没&&日志完整性检查
	rfLastLogIndex := rf.getLastLogIndex()
	rfLastLogTerm := rf.getLastLogTerm()
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rfLastLogTerm || (args.LastLogTerm == rfLastLogTerm && args.LastLogIndex >= rfLastLogIndex)) {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		rf.ClearDelayTime()
	} else {
		reply.VoteGranted = false
	}
	/*
		DPrintf("%d Reveive RequestVote From %d, args.LastLogTerm: %d curTerm: %d\n"+
			"args.LstLogIndex: %d, rf.LastLogIndex: %d  return %v\n",
			rf.me, args.CandidateId, args.LastLogTerm, rf.currentTerm,
			args.LastLogIndex, rf.getLastLogIndex(),
			reply.VoteGranted)
	*/
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	//这是用来加速解决日志冲突的  raft原文说这个没必要，
	//但是6.824好像有必要,不用这个过不去3C的后面几个测试
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%d Recevive AppendEntries From %d, "+
		"Term %d, lenLogs: %d\n", rf.me, args.LeaderId, args.Term, len(args.Entries))
	rf.mu.Lock()
	LockLog("%d AppendEntries GetLock\n", rf.me)
	defer LockLog("%d AppendEntries UnLock\n", rf.me)
	defer rf.mu.Unlock()
	//DPrintf("-------AppendEntries: %d term: %d from %v\n", rf.me, rf.currentTerm, args)
	reply.Term = rf.currentTerm
	// 1.Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if rf.GetStateType() != Follower && args.Term >= rf.currentTerm {
		rf.SetStateType(Follower)
		rf.votedFor = -1
		rf.persist()
	}

	rf.ClearDelayTime() // 清空delayTime
	// 注意这个清空在确认对方是Leader后就要清空
	// 因为下面第二步如果有很多日志不一致，会花很多时间来同步信息，会把领导选举卡到超时
	offset := rf.getLogOffset()
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	logTerm := rf.getLogTerm(args.PrevLogIndex)
	//DPrintf("%d AppendEntries, args.PrevIndex: %d, offset: %d, lenLog: %d, lastLogIndex: %d\n",
	//rf.me, args.PrevLogIndex, offset, len(rf.log), rf.getLastLogIndex())
	if args.PrevLogIndex > rf.getLastLogIndex() || logTerm != args.PrevLogTerm {
		reply.Success = false
		if args.PrevLogIndex > rf.getLastLogIndex() || logTerm == -1 {
			//DPrintf("%d 太短了, back Term: -1, confictIndex: %d\n", rf.me, rf.getLastLogIndex())
			reply.ConflictTerm = -1
			reply.ConflictIndex = rf.getLastLogIndex()
		} else {
			reply.ConflictTerm = logTerm
			index := args.PrevLogIndex - 1
			//DPrintf("args.PrevLogIndex %d \n", args.PrevLogIndex)
			for index-offset >= 0 && rf.log[index-offset].ApplyMsg.Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index
			//DPrintf("id: %d args.PrevLogIndex: %d, rf.getLastLogIndex(): %d, offset: %d",
			//rf.me, args.PrevLogIndex, rf.getLastLogIndex(), rf.getLogOffset())
			//DPrintf("%d 切割到了 term: %d index %d\n", rf.me, reply.ConflictTerm, reply.ConflictIndex)
		}

		return
	}

	// 3.If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	newEntriesPos := 0
	for i := args.PrevLogIndex + 1; i <= rf.getLastLogIndex() && newEntriesPos < len(args.Entries); i, newEntriesPos = i+1, newEntriesPos+1 {
		if rf.log[i-offset].ApplyMsg.Term != args.Entries[newEntriesPos].ApplyMsg.Term {
			rf.log = rf.log[:i-offset]
			rf.persist()
			break
		}
	}
	// 4.Append any new entries not already in the log
	if len(args.Entries) > 0 {
		//DPrintf("%d will Add Entries: %v\n", rf.me, args.Entries[newEntriesPos:])
		rf.log = append(rf.log, args.Entries[newEntriesPos:]...)
		rf.persist()
	}
	//DPrintf("id: %d Log Entries: %v\n", rf.me, rf.log)
	// 5.If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	rf.commitIndex = Min(args.LeaderCommit, rf.getLastLogIndex())
	//DPrintf("%d commitIndex: %d, from min(leaderCommit: %d, lastLogIndex: %d)\n", rf.me, rf.commitIndex,
	//args.LeaderCommit, rf.getLastLogIndex())
	DPrintf("%d AppendEntries Signal", rf.me)
	rf.applyCond.Signal()
	reply.Success = true
	rf.currentTerm = args.Term
	rf.persist()
}

func (rf *Raft) SendAppendEntries(server, targetLogIndex int, args *AppendEntriesArgs) {
	//DPrintf("%d in Term %d go Send to %d\n", rf.me, rf.currentTerm, idx)
	reply := AppendEntriesReply{}
	if rf.peers[server].Call("Raft.AppendEntries", args, &reply) {
		rf.mu.Lock()
		//DPrintf("Receive %v From %v\n", reply.Success, server)
		if reply.Success {
			//这里不能直接调用最末端的LogIndex
			//试想直接使用最末端的LogIndex, 在发送心跳rpc中途，突然有客户端操作进入，获得的最末端的LogIndex是包括新加入的请求的
			//matchIndex和nextIndex会直接被直接拉高到认为客户端操作已经匹配完成
			rf.nextIndex[server] = targetLogIndex
			rf.matchIndex[server] = targetLogIndex - 1
			//DPrintf("id: %d Append Entries 成功匹配 nextIndex: %d\n", server, targetLogIndex)
			rf.LeaderRefreshCommitIndex()
			if rf.lastApplied < rf.commitIndex {
				DPrintf("%d SendAppendEntries Signal", rf.me)
				rf.applyCond.Signal()
			}
		} else {
			//DPrintf("Id: %d, ConflictTerm: %d, ConflictIndex: %d\n", server, reply.ConflictTerm, reply.ConflictIndex)
			if rf.nextIndex[server] > 1 {
				if reply.ConflictTerm == -1 {
					rf.nextIndex[server] = reply.ConflictIndex + 1
				} else {
					rf.nextIndex[server] = reply.ConflictIndex + 1
					conflictTerm := reply.ConflictTerm
					//DPrintf("lenLog: %d, snapTerm:%d snapIndex%d\n",
					//len(rf.log), rf.log[0].ApplyMsg.SnapshotTerm, rf.log[0].ApplyMsg.SnapshotIndex)
					for i := len(rf.log) - 1; i >= 1; i-- {
						//DPrintf("log[%d].Term == %d\n", i, rf.log[i].Term)
						if rf.log[i].ApplyMsg.Term == conflictTerm {
							rf.nextIndex[server] = rf.log[i].ApplyMsg.CommandIndex
							break
						}
					}
					//DPrintf("rf.nextIndex[%d] change to %d\n", server, reply.ConflictIndex+1)
				}
				//DPrintf("id: %d Append Entries 不匹配 nextIndex: %d\n", server, rf.nextIndex[server])
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.SetStateType(Follower)
				rf.votedFor = -1
				rf.persist()
			}
		}
		rf.mu.Unlock()
	}
	return
}

func (rf *Raft) GoSendAppendEntries() {
	for !rf.killed() && rf.GetStateType() == Leader {
		// 如果服务器是leader，不停的发送包给所有的follower
		<-rf.heartTimer.C
		rf.BroadCastAppendEntries()
		rf.ResetHeartTimer(100)
	}
}
func (rf *Raft) BroadCastAppendEntries() {
	for peer, _ := range rf.peers {
		if peer == rf.me {
			continue
		}
		if rf.GetStateType() != Leader {
			return
		}
		rf.mu.Lock()
		offset := rf.getLogOffset()
		nextIndex := rf.nextIndex[peer]
		logEntries := make([]Log, 0)
		//DPrintf("nextIndex: %d, lastLogIndex: %d, offset: %d\n", nextIndex, rf.getLastLogIndex(), offset)
		if nextIndex <= offset {
			// 这里如果nextIndex还很小，而lastIndex和offset已经拉高了，
			// 说明nextIndex已经在快照中了，需要InstallSnapshotRPC来协助了
			// DPrintf("%d GoSendInstallSnapshot: to %d\n", rf.me, peer)
			go rf.GoSendInstallSnapshot(peer)
			rf.mu.Unlock()
			continue
		}
		for i := nextIndex; i <= rf.getLastLogIndex(); i++ {
			logEntries = append(logEntries, rf.log[i-offset])
		}
		prevLogIndex, prevLogTerm := 0, 0
		prevLog := rf.getLog(nextIndex - 1)
		if prevLog.ApplyMsg.CommandValid {
			prevLogIndex = rf.log[nextIndex-1-offset].ApplyMsg.CommandIndex
			prevLogTerm = rf.log[nextIndex-1-offset].ApplyMsg.Term
		} else if prevLog.ApplyMsg.SnapshotValid {
			prevLogIndex = rf.log[nextIndex-1-offset].ApplyMsg.SnapshotIndex
			prevLogTerm = rf.log[nextIndex-1-offset].ApplyMsg.SnapshotTerm
		} else {
			log2.Fatalf("%d Find Error\n", rf.me)
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      logEntries,
			LeaderCommit: rf.commitIndex,
		}
		targetLogIndex := rf.getLastLogIndex() + 1
		rf.mu.Unlock()
		// 这里发现一种偶现情况，来自3b的TestBackup3B
		// 如果把args的生成丢到协程里面做时
		// 当一个落后&临时断线的Leader复活了，给其他peers发送appendRPC时
		// 一个遍历的前方的rpc已经回包告诉这个落后Leader落后了，Leader会修改Term和状态
		// 此时其他协程其实并不知道Leader已经变成follower了，而且还会读到这个节点获得的最新Term
		// 此时发送的rpc会被检查并通过，因为term已经变成正常的了
		go rf.SendAppendEntries(peer, targetLogIndex, &args)
	}
}

func (rf *Raft) LeaderRefreshCommitIndex() {
	newCommitIndex := rf.commitIndex + 1
	offset := rf.getLogOffset()
	for {
		cnt := 1
		for i := 0; i < len(rf.matchIndex); i++ {
			if newCommitIndex <= rf.matchIndex[i] {
				cnt++
			}
		}
		if cnt <= len(rf.peers)/2 {
			break
		}
		newCommitIndex++
	}
	newCommitIndex--
	if newCommitIndex-offset <= 0 || newCommitIndex-offset >= len(rf.log) ||
		rf.log[newCommitIndex-offset].ApplyMsg.Term != rf.currentTerm {
		// 如果要提交的log还是在老任期的，是不予提交的
		// 只有节点在当前任期内，才予以提交（并会把老节点一起提交），详情5.4.2
		return
	}
	rf.commitIndex = newCommitIndex
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	LockLog("%d Start GetLock\n", rf.me)
	defer LockLog("%d Start UnLock\n", rf.me)
	defer rf.mu.Unlock()
	if rf.GetStateType() != Leader {
		return -1, -1, false
	}
	term := rf.currentTerm
	//DPrintf("command %v to %d Term: %v\n", command, rf.me, rf.currentTerm)
	// Your code here (3B).
	newLog := Log{
		ApplyMsg: ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: rf.getLastLogIndex() + 1,
			Term:         term,
		},
	}
	rf.log = append(rf.log, newLog)
	rf.persist()
	defer func() {
		rf.ResetHeartTimer(0)
	}()
	return newLog.ApplyMsg.CommandIndex, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		outTime := 500 + (rand.Int63() % 300)
		if rf.GetStateType() != Leader && rf.GetDelayTime() > outTime { // 如果发现超时，还没收到leader发来的心跳，那开启领导选举
			rf.SetStateType(Candidate)
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.persist()
			rf.mu.Unlock()
			rf.ClearDelayTime()
			//DPrintf("%d 开始竞选 Term: %d\n", rf.me, rf.currentTerm)

			var voteCnt int
			voteCh := make(chan bool)
			go func() {
				for vote := range voteCh {
					if vote {
						voteCnt++
						if voteCnt > len(rf.peers)/2 && rf.GetStateType() == Candidate {
							DPrintf("%d Be Leader\n", rf.me)
							rf.SetStateType(Leader)
							rf.mu.Lock()
							rf.votedFor = -1
							rf.persist()
							for i := 0; i < len(rf.peers); i++ {
								rf.nextIndex[i] = rf.getLastLogIndex() + 1
								rf.matchIndex[i] = 0
							}
							rf.mu.Unlock()
							go rf.GoSendAppendEntries()
							return
						}
					}
				}
			}()
			voteCh <- true
			var voteChMu sync.Mutex
			closed := false
			for index, _ := range rf.peers {
				if index == rf.me {
					continue
				}
				rf.mu.Lock()
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.getLastLogIndex(),
					LastLogTerm:  rf.getLastLogTerm(),
				}
				rf.mu.Unlock()
				go func(idx int) {
					reply := RequestVoteReply{}
					if rf.sendRequestVote(idx, &args, &reply) {
						voteChMu.Lock()
						if reply.VoteGranted && !closed {
							voteCh <- true
						} else {
							rf.mu.Lock()
							if reply.Term > rf.currentTerm {
								//如果受到比自己的term更高的peer的回复，转变成Follower,放弃竞选
								rf.SetStateType(Follower)
								rf.votedFor = -1
								rf.currentTerm = reply.Term
								rf.persist()
								if !closed {
									close(voteCh)
								}
								closed = true
							}
							rf.mu.Unlock()
						}
						voteChMu.Unlock()
					}
				}(index)
			}
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		if rf.GetStateType() != Leader {
			// 如果是Follower，超时说明没收到Leader的心跳，开启选举
			// 如果是Candidate，超时说明这次选举没结果，开启选举
			// 所以两个状态都需要计算超时
			rf.AddDelayTime(ms)
		} else {
			rf.ClearDelayTime()
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		offset, commitIndex, lastApplied := rf.getLogOffset(), rf.commitIndex, rf.lastApplied
		entries := make([]Log, commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied-offset+1:commitIndex-offset+1])
		DPrintf("%d entries Size: %d, from %d to %d\n", rf.me, len(entries), lastApplied, commitIndex)
		rf.mu.Unlock()
		// apply 到状态机中
		for _, entry := range entries {
			rf.applyCh <- entry.ApplyMsg
			DPrintf("%d Apply index: %d to State Machine\n", rf.me, entry.ApplyMsg.CommandIndex)
		}
		rf.mu.Lock()
		DPrintf("%v applies entries %v-%v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		Persister:   persister,
		me:          me,
		votedFor:    -1,
		lastApplied: 0,
		state:       Follower,
		log:         make([]Log, 1),
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		applyCh:     applyCh,
		heartTimer:  time.NewTimer(0),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.log[0].ApplyMsg.CommandValid = true
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	return rf
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
