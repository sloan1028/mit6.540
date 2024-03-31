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
	//	"bytes"
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
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []string // You'll also need to define a struct to hold information about each log entry.
	logIndex    int

	commitIndex int
	lastApplied int

	// Reinitialized after election
	nextIndex  []int
	matchIndex []int

	stateMu     sync.Mutex
	state       StateType
	delayTimeMu sync.Mutex
	delayTime   int64
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.GetStateType() == Leader
	//DPrintf("Id: %d, IsLeader: %v Term: %d\n", rf.me, isleader, rf.currentTerm)
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm { // 如果服务器发现自己过时了
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.SetStateType(Follower)
	}
	// 这里需要改进的日志完整性检查（示例略）
	// if args.LastLogTerm < rf.lastLogTerm ||
	//   (args.LastLogTerm == rf.lastLogTerm && args.LastLogIndex < rf.lastLogIndex) {
	//     reply.VoteGranted = false
	//     return
	// }

	// 接下来检验请求的Candidate能不能通过
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.LastLogIndex >= rf.logIndex {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
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
	Entries      []string
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintf("%d Reveive AppendEntries From %d, Term %d\n", rf.me, args.LeaderId, args.Term)
	// 现在只用来收发心跳包
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	//DPrintf("%d is A %v, Term From %d to %d\n", rf.me, rf.GetStateType(), rf.currentTerm, args.Term)
	if rf.GetStateType() != Follower && args.Term >= rf.currentTerm {
		rf.SetStateType(Follower)
		rf.votedFor = -1
	}
	if args.LeaderCommit > rf.commitIndex {

	}
	reply.Success = true
	rf.currentTerm = args.Term
	rf.ClearDelayTime() // 收到后清空delayTime
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) GoSendAppendEntries() {
	for rf.killed() == false && rf.GetStateType() == Leader {
		// 如果服务器是leader，不停的发送包给所有的follower
		for index, _ := range rf.peers {
			if index == rf.me {
				continue
			}
			rf.mu.Lock()
			//DPrintf("Leader %d Send AppendEntries to %d, term: %d\n", rf.me, index, rf.currentTerm)
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			rf.mu.Unlock()
			go func(idx int) {
				reply := AppendEntriesReply{}
				if rf.SendAppendEntries(idx, &args, &reply) {
					if reply.Success {

					} else {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.SetStateType(Follower)
							rf.votedFor = -1
						}
						rf.mu.Unlock()
					}
				}
			}(index)
		}
		time.Sleep(100 * time.Millisecond)
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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
		outTime := 1000 + (rand.Int63() % 600)
		if rf.GetStateType() != Leader && rf.GetDelayTime() > outTime { // 如果发现超时，还没收到leader发来的心跳，那开启领导选举
			rf.SetStateType(Candidate)
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
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
							//DPrintf("%d Be Leader\n", rf.me)
							rf.SetStateType(Leader)
							rf.mu.Lock()
							rf.votedFor = -1
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
					LastLogIndex: rf.logIndex,
					LastLogTerm:  rf.currentTerm,
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
							if reply.Term > rf.currentTerm && rf.GetStateType() == Candidate {
								//如果受到比自己的term更高的peer的回复，转变成Follower,放弃竞选
								rf.SetStateType(Follower)
								rf.votedFor = -1
								rf.currentTerm = reply.Term
								closed = true
								close(voteCh)
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
		peers:     peers,
		persister: persister,
		me:        me,
		votedFor:  -1,
		state:     Follower,
	}

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
