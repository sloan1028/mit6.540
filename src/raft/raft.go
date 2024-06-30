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
	"6.5840/labrpc"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
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
	log         []ApplyMsg // len(log)代表的是下一个进入log的index，此时获得的这个值上的log是空的

	commitIndex int
	lastApplied int

	// Reinitialized after election
	nextIndex  []int
	matchIndex []int

	state       StateType
	delayTimeMu sync.Mutex
	delayTime   int64

	applyCh                chan ApplyMsg
	isNeedApplyingSnapshot bool

	heartTimer *time.Timer
}

func (rf *Raft) ResetHeartTimer(timeStamp int) {
	rf.heartTimer.Reset(time.Duration(timeStamp) * time.Millisecond)
}

func (rf *Raft) GetStateType() (state StateType) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	state = rf.state
	return state
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
		if rf.log[0].CommandValid {
			return rf.log[0].CommandIndex
		}
		if rf.log[0].SnapshotValid {
			return rf.log[0].SnapshotIndex
		}
	}
	log.Fatalf("%d getLogOffset error\n", rf.me)
	return -10086
}

func (rf *Raft) getLastLogIndex() int {
	logEntry := rf.log[len(rf.log)-1]
	if logEntry.CommandValid {
		return logEntry.CommandIndex
	}
	if logEntry.SnapshotValid {
		return logEntry.SnapshotIndex
	}
	log.Fatalf("%d getLastLogIndex error\n", rf.me)
	return -10086
}

func (rf *Raft) getLastLogTerm() int {
	logEntry := rf.log[len(rf.log)-1]
	if logEntry.CommandValid {
		return logEntry.Term
	}
	if logEntry.SnapshotValid {
		return logEntry.SnapshotTerm
	}
	log.Fatalf("%d getLastLogTerm error\n", rf.me)
	return -10086
}

func (rf *Raft) getLogTerm(logIndex int) int {
	offset := rf.getLogOffset()
	if logIndex-offset >= len(rf.log) {
		return -1
	}
	if logIndex-offset > 0 {
		return rf.log[logIndex-offset].Term
	} else if logIndex-offset == 0 {
		if rf.log[0].SnapshotValid {
			return rf.log[0].SnapshotTerm
		}
		return rf.log[0].Term
	}
	return -1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
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
	e.Encode(rf.log[0].SnapshotTerm)
	e.Encode(rf.log[0].SnapshotIndex)
	raftState := w.Bytes()

	rf.Persister.Save(raftState, rf.log[0].Snapshot)
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
	var logEntries []ApplyMsg
	var SnapshotTerm int
	var SnapshotIndex int
	if d.Decode(&curTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logEntries) != nil ||
		d.Decode(&SnapshotTerm) != nil || d.Decode(&SnapshotIndex) != nil {
		log.Fatalf("%v Decode error %v\n", d, data)
	} else {
		rf.currentTerm = curTerm
		rf.votedFor = voteFor
		rf.log = append(rf.log, logEntries...)
		Debug(dPersist, "ID: %d ReadPersist log: %v\n", rf.me, rf.log)
		if SnapshotIndex != 0 {
			rf.log[0].CommandValid = false
			rf.log[0].SnapshotValid = true
			rf.log[0].SnapshotIndex = SnapshotIndex
			rf.log[0].SnapshotTerm = SnapshotTerm
			// 这里重开时要把快照拉回来，否则下次再存的时候，这里是空的直接把快照弄没了。Bug From 3D最后一个TestSnapshotInit3D
			rf.log[0].Snapshot = rf.Persister.ReadSnapshot()
			rf.lastApplied = SnapshotIndex
			rf.commitIndex = SnapshotIndex
		}
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	offset := rf.getLogOffset()
	if index-offset <= 0 || index-offset >= len(rf.log) {
		return
	}
	term := rf.log[index-offset].Term
	newLog := make([]ApplyMsg, 1)
	newLog[0] = ApplyMsg{
		SnapshotValid: true,
		SnapshotIndex: index,
		SnapshotTerm:  term,
		Snapshot:      snapshot,
	}
	idx := 1
	for idx < len(rf.log) {
		applyMsg := rf.log[idx]
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
	Debug(dSnap, "ID: %d Do SnapShot index: %d, LogLen to %d, lastIndex: %d, lastTerm: %d\n",
		rf.me, index, len(rf.log), rf.getLastLogIndex(), rf.getLastLogTerm())
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
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// 1.Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if rf.state != Follower && args.Term >= rf.currentTerm {
		rf.state = Follower
		rf.votedFor = -1
	}
	rf.currentTerm = args.Term
	rf.persist()

	// 过时的Snapshot不要了， 如果这里要了可能会导致超界
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	newSnapshot := ApplyMsg{
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
		Snapshot:      args.Data,
	}
	newApplyMsg := newSnapshot

	// 2. Save snapshot file, discard any existing or partial snapshot with a smaller index
	if rf.log[0].CommandValid ||
		(rf.log[0].SnapshotValid && rf.log[0].SnapshotIndex < args.LastIncludedIndex) {
		rf.log[0] = newApplyMsg
	}
	// 3. If existing log entry has same index and term as snapshot’s last included entry,
	// retain log entries following it and reply
	for i := 1; i < len(rf.log); i++ {
		log := rf.log[i]
		if log.CommandIndex == args.LastIncludedIndex && log.Term == args.LastIncludedTerm {
			rf.lastApplied = args.LastIncludedIndex
			rf.commitIndex = args.LastIncludedIndex

			saveLog := rf.log[i+1:]
			rf.log = rf.log[:1]
			rf.log = append(rf.log, saveLog...)

			rf.persist()
			rf.isNeedApplyingSnapshot = true
			rf.applyCond.Signal()
			return
		}
	}
	// 4. Discard the entire log
	rf.log = rf.log[:1]

	// 5.Reset state machine using snapshot contents
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.persist()
	rf.isNeedApplyingSnapshot = true
	rf.applyCond.Signal()
}

func (rf *Raft) GoSendInstallSnapshot(peer int) {
	if rf.GetStateType() != Leader {
		return
	}
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log[0].SnapshotIndex,
		LastIncludedTerm:  rf.log[0].SnapshotTerm,
		Data:              rf.Persister.ReadSnapshot(),
	}
	applySnapshotIndex := rf.log[0].SnapshotIndex
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	if rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply) {
		rf.mu.Lock()
		rf.nextIndex[peer] = applySnapshotIndex + 1
		rf.matchIndex[peer] = applySnapshotIndex
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
		}
		rf.mu.Unlock()
	}
	return

}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		Debug(dVote, "ID: %d Receive RequestVote From %d reply: %v\n", rf.me, args.CandidateId, reply)
		return
	}
	if args.Term > rf.currentTerm { // 如果服务器发现自己过时了
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
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
	Debug(dVote, "ID: %d Receive RequestVote From %d, ID'S lastLogTerm: %d, lastLogIndex: %d, reply: %v\n",
		rf.me, args.CandidateId, rfLastLogTerm, rfLastLogIndex, reply)
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
	Entries      []ApplyMsg
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	//这是用来加速解决日志冲突的  raft原文说这个没必要，
	//但是6.824有必要,不用这个过不去3C的后面几个测试
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer Debug(dInfo, "ID: %d Receive AppendEntries From %d, Reply %v\n", rf.me, args.LeaderId, reply)
	reply.Term = rf.currentTerm
	// 1.Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if rf.state != Follower && args.Term >= rf.currentTerm {
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}

	rf.ClearDelayTime() //确认对方是Leader后就要重置超时 因为下面第二步如果有很多日志不一致，会花很多时间来同步信息，会把领导选举卡到超时
	offset := rf.getLogOffset()
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	logTerm := rf.getLogTerm(args.PrevLogIndex)
	if args.PrevLogIndex > rf.getLastLogIndex() || logTerm != args.PrevLogTerm {
		reply.Success = false
		if args.PrevLogIndex > rf.getLastLogIndex() || logTerm == -1 {
			reply.ConflictTerm = -1
			reply.ConflictIndex = rf.getLastLogIndex()
		} else {
			reply.ConflictTerm = logTerm
			index := args.PrevLogIndex - 1
			for index-offset >= 0 && rf.log[index-offset].Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index
		}
		return
	}

	// 3.If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	newEntriesPos := 0
	for i := args.PrevLogIndex + 1; i <= rf.getLastLogIndex() && newEntriesPos < len(args.Entries); i, newEntriesPos = i+1, newEntriesPos+1 {
		if rf.log[i-offset].Term != args.Entries[newEntriesPos].Term {
			rf.log = rf.log[:i-offset]
			break
		}
	}
	// 4.Append any new entries not already in the log
	if len(args.Entries) > 0 {
		Debug(dLog, "ID: %d Receive AppendEntriesFrom %d will Add Entries: %v\n",
			rf.me, args.LeaderId, args.Entries[newEntriesPos:])
		rf.log = append(rf.log, args.Entries[newEntriesPos:]...)
	}
	// 5.If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	rf.commitIndex = Min(args.LeaderCommit, rf.getLastLogIndex())
	rf.applyCond.Signal()
	reply.Success = true
	rf.currentTerm = args.Term
	rf.persist()
}

func (rf *Raft) SendAppendEntries(server, targetLogIndex int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	Debug(dLog, "ID: %d SendAppendEntries to %d, Term: %d, PrevLogTerm: %d, PrevLogIndex: %d\n",
		rf.me, server, args.Term, args.PrevLogTerm, args.PrevLogIndex)
	if rf.peers[server].Call("Raft.AppendEntries", args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Success {
			//这里不能直接调用最末端的LogIndex
			//试想直接使用最末端的LogIndex, 在发送心跳rpc中途，突然有客户端操作进入，获得的最末端的LogIndex是包括新加入的请求的
			//matchIndex和nextIndex会直接被直接拉高到认为客户端操作已经匹配完成
			rf.nextIndex[server] = targetLogIndex
			rf.matchIndex[server] = targetLogIndex - 1
			rf.LeaderRefreshCommitIndex()
			if rf.lastApplied < rf.commitIndex {
				rf.applyCond.Signal()
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.persist()
				return
			}
			Debug(dLog, "Id: %d ConflictTerm: %d, ConflictIndex: %d\n", server, reply.ConflictTerm, reply.ConflictIndex)
			if rf.nextIndex[server] > 1 {
				if reply.ConflictTerm == -1 {
					rf.nextIndex[server] = reply.ConflictIndex + 1
				} else {
					rf.nextIndex[server] = reply.ConflictIndex + 1
					conflictTerm := reply.ConflictTerm
					for i := len(rf.log) - 1; i >= 1; i-- {
						if rf.log[i].Term == conflictTerm {
							rf.nextIndex[server] = rf.log[i].CommandIndex
							break
						}
					}
				}
			}
		}
	}
	return
}

func (rf *Raft) GoSendAppendEntries() {
	for !rf.killed() && rf.GetStateType() == Leader {
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
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		offset := rf.getLogOffset()
		nextIndex := rf.nextIndex[peer]
		logEntries := make([]ApplyMsg, 0)
		if nextIndex <= offset {
			// 这里如果nextIndex还很小，而lastIndex和offset已经拉高了，
			// 说明nextIndex已经在快照中了，需要InstallSnapshotRPC来协助了
			go rf.GoSendInstallSnapshot(peer)
			rf.mu.Unlock()
			continue
		}
		for i := nextIndex; i <= rf.getLastLogIndex(); i++ {
			logEntries = append(logEntries, rf.log[i-offset])
		}
		prevLogIndex, prevLogTerm := nextIndex-1, 0
		if nextIndex-1-offset >= len(rf.log) {
			Debug(dError, "ID: %d logIndex: %d offset: %d, lenLog: %d\n", rf.me, nextIndex-1, offset, len(rf.log))
			rf.mu.Unlock()
			return
		}
		prevLog := rf.log[nextIndex-1-offset]
		if prevLog.CommandValid {
			prevLogTerm = rf.log[nextIndex-1-offset].Term
		} else if prevLog.SnapshotValid {
			prevLogTerm = rf.log[nextIndex-1-offset].SnapshotTerm
		} else {
			log.Fatalf("%d Log Error CommandValid SnapValid All false\n", rf.me)
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
		rf.log[newCommitIndex-offset].Term != rf.currentTerm {
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
	defer func() {
		rf.mu.Unlock()
		rf.ResetHeartTimer(0)
	}()
	if rf.state != Leader {
		return -1, -1, false
	}
	term := rf.currentTerm
	// Your code here (3B).
	newLog := ApplyMsg{
		CommandValid: true,
		Command:      command,
		CommandIndex: rf.getLastLogIndex() + 1,
		Term:         term,
	}
	Debug(dLog, "ID: %d Get A Command In Term: %d, CommandIndex: %d, Command: %v\n",
		rf.me, rf.currentTerm, newLog.CommandIndex, newLog.Command)
	rf.log = append(rf.log, newLog)
	rf.persist()
	return newLog.CommandIndex, term, true
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
			rf.mu.Lock()
			rf.state = Candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.persist()
			voteTerm := rf.currentTerm
			rf.mu.Unlock()
			rf.ClearDelayTime()
			Debug(dVote, "ID: %d 开始竞选 Term: %d\n", rf.me, rf.currentTerm)

			var voteCnt int
			voteCh := make(chan bool)
			go func(int) {
				for vote := range voteCh {
					if vote {
						voteCnt++
						rf.mu.Lock()
						if voteCnt+1 > len(rf.peers)/2 && rf.state == Candidate && rf.currentTerm == voteTerm {
							Debug(dVote, "ID: %d Be Leader, Term: %d\n", rf.me, rf.currentTerm)
							rf.state = Leader
							rf.persist()
							for i := 0; i < len(rf.peers); i++ {
								rf.nextIndex[i] = rf.getLastLogIndex() + 1
								rf.matchIndex[i] = 0
							}
							rf.mu.Unlock()
							go rf.GoSendAppendEntries()
							return
						}
						rf.mu.Unlock()
					}
				}
			}(voteTerm)
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
					Debug(dVote, "ID: %d Send RequestVote to %d, args: %v\n", rf.me, idx, args)
					if rf.sendRequestVote(idx, &args, &reply) {
						voteChMu.Lock()
						if reply.VoteGranted && !closed {
							Debug(dVote, "ID: %d, LeaderElection Get One Vote From %d\n", rf.me, idx)
							voteCh <- true
						} else {
							rf.mu.Lock()
							if reply.Term > rf.currentTerm {
								//如果受到比自己的term更高的peer的回复，转变成Follower,放弃竞选
								rf.state = Follower
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
			rf.AddDelayTime(ms)
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for !rf.isNeedApplyingSnapshot && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.isNeedApplyingSnapshot {
			rf.isNeedApplyingSnapshot = false
			snapshot := rf.log[0]
			rf.mu.Unlock()
			rf.applyCh <- snapshot
			continue
		}
		offset, commitIndex, lastApplied := rf.getLogOffset(), rf.commitIndex, rf.lastApplied
		entries := make([]ApplyMsg, commitIndex-lastApplied)
		copy(entries, rf.log[Max(1, lastApplied-offset+1):Max(1, commitIndex-offset+1)])
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- entry
			Debug(dLog, "ID: %d Apply index: %d command: %v\n", rf.me, entry.CommandIndex, entry.Command)
		}
		rf.mu.Lock()
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		Persister:   persister,
		me:          me,
		votedFor:    -1,
		lastApplied: 0,
		state:       Follower,
		log:         make([]ApplyMsg, 1),
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		applyCh:     applyCh,
		heartTimer:  time.NewTimer(0),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.log[0].CommandValid = true

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.applier()
	return rf
}
