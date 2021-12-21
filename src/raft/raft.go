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
	"bytes"
	"encoding/gob"
	"labrpc"
	"math"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
	Term        int    //任期
}

type StateType int //server状态

const (
	FOLLOWER             StateType = 0
	CANDIDATE            StateType = 1
	LEADER               StateType = 2
	ELECTION_MIN_TIME_MS int64     = 150
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	leader int
	state  StateType

	//Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []ApplyMsg

	//Volatile state on all servers:
	commitIndex int
	lastApplied int

	//Volatile state on leaders:(Reinitialized after election)
	nextIndex  []int
	matchIndex []int

	lastVotedTimeUnix    int64
	applyCh              chan ApplyMsg
	timer                *time.Timer
	candidateVoteMeCount int
}

func (rf *Raft) resetTimer(timeMs int64) {
	rf.timer.Reset(time.Millisecond * time.Duration(timeMs))
}

func (rf *Raft) autoResetTimer() {
	if rf.state == LEADER {
		rf.resetTimer(ELECTION_MIN_TIME_MS)
	} else if rf.state == FOLLOWER {
		rf.resetTimer(2 * ELECTION_MIN_TIME_MS)
	} else if rf.state == CANDIDATE {
		electionWaitTimeMs := ELECTION_MIN_TIME_MS + rand.Int63n(ELECTION_MIN_TIME_MS)
		rf.resetTimer(electionWaitTimeMs)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here.
	term = rf.currentTerm
	if rf.state == LEADER {
		isLeader = true
	} else {
		isLeader = false
	}
	return term, isLeader
}

func (rf *Raft) initLeaderVolatileState() {
	peerNum := len(rf.peers)
	rf.nextIndex = make([]int, peerNum, peerNum)
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.lastApplied + 1
	}
	rf.matchIndex = make([]int, peerNum, peerNum)
}

func (rf *Raft) broadcastHeartBeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.sendAppendEntry(i)
		}
	}
}

func (rf *Raft) switchState(serverType StateType) {
	rf.state = serverType
	if serverType == FOLLOWER {
		rf.votedFor = -1
		rf.autoResetTimer()
	}
	if serverType == LEADER {
		rf.autoResetTimer()
		rf.initLeaderVolatileState()
		rf.leader = rf.me
		rf.broadcastHeartBeat()
	}
	//SERVER_TYPE_CANDIDATE 无需其他操作
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.commitIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data != nil {
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		d.Decode(&rf.currentTerm)
		d.Decode(&rf.votedFor)
		d.Decode(&rf.log)
		d.Decode(&rf.commitIndex)
	}
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int //candidate’s term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry
	LastLogTerm  int //term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm { //Reply false if term < currentTerm
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.persist()
		return
	}
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId { //If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
		if args.LastLogIndex >= rf.commitIndex {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
	}
	rf.persist()
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntry struct {
	Term         int        //leader’s term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []ApplyMsg //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        //leader’s commitIndex
}

type AppendEntryReply struct {
	Term                 int  //currentTerm, for leader to update itself
	Success              bool //true if follower contained entry matching prevLogIndex and prevLogTerm
	FollowerLastLogIndex int
}

func (rf *Raft) AppendEntries(args AppendEntry, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		rf.switchState(FOLLOWER)
		rf.currentTerm = args.Term
		rf.leader = args.LeaderId
		reply.Term = rf.currentTerm
		reply.Success = true

		//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		logLen := len(rf.log)
		if logLen < args.PrevLogIndex || (args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
			reply.Success = false
		} else {
			//If an existing entry conflicts with a new one (same index
			//but different terms), delete the existing entry and all that
			//follow it
			if args.PrevLogIndex < logLen {
				rf.log = rf.log[:args.PrevLogIndex]
			}
			//Append any new entries not already in the log
			rf.log = append(rf.log, args.Entries...)
			rf.persist()

			//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
			if args.LeaderCommit > rf.commitIndex {
				newCommitIndex := int(math.Min(float64(args.LeaderCommit), float64(len(rf.log))))
				for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
					rf.persist()
					rf.applyCh <- rf.log[i-1]
				}
				rf.commitIndex = newCommitIndex
			}
		}
	}
	reply.FollowerLastLogIndex = len(rf.log)
	rf.autoResetTimer()
	rf.persist()
}

func (rf *Raft) handleAppendEntries(server int, reply AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.switchState(FOLLOWER)
		return
	}
	if reply.Success == true {
		rf.nextIndex[server] = reply.FollowerLastLogIndex + 1
		rf.matchIndex[server] = reply.FollowerLastLogIndex
	} else {
		rf.nextIndex[server] = rf.nextIndex[server] - 1
		rf.matchIndex[server] = rf.matchIndex[server] - 1
		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
		if rf.matchIndex[server] < 0 {
			rf.matchIndex[server] = 0
		}
	}
	N := -1
	count := 0
	for n := reply.FollowerLastLogIndex; n > rf.commitIndex; n-- {
		count = 0
		for i := 0; i < len(rf.matchIndex); i++ {
			if rf.matchIndex[i] >= n {
				count++
			}
		}
		if count >= len(rf.matchIndex)/2+1 {
			N = n
			break
		}
	}
	for i := rf.commitIndex + 1; i <= N; i++ {
		rf.commitIndex = i
		rf.applyCh <- rf.log[i-1]
	}
	rf.persist()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if isLeader {
		index = len(rf.log) + 1
		//term = rf.currentTerm
		//本地写日志
		logEntry := ApplyMsg{
			Index:       index,
			Command:     command,
			Term:        term,
			UseSnapshot: false,
			Snapshot:    nil,
		}
		rf.log = append(rf.log, logEntry)
		rf.nextIndex[rf.me] = len(rf.log) + 1
		rf.matchIndex[rf.me] = len(rf.log)
		rf.persist()
		//发送AppendEntry
		rf.broadcastHeartBeat()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) sendAppendEntry(followerId int) {
	go func(targetServer int) {
		rf.mu.Lock()
		if rf.state != LEADER {
		}
		prevLogIndex := 0
		prevLogTerm := 0
		if len(rf.log) > 0 {
			prevLogIndex = rf.nextIndex[targetServer] - 1
			if prevLogIndex > 0 {
				prevLogTerm = rf.log[prevLogIndex-1].Term
			}
		}
		appendEntry := AppendEntry{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      rf.log[prevLogIndex:],
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()
		var reply AppendEntryReply
		ok := rf.peers[targetServer].Call("Raft.AppendEntries", appendEntry, &reply)
		if ok {
			rf.handleAppendEntries(targetServer, reply)
		}
	}(followerId)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.leader = -1
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.lastVotedTimeUnix = -1
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]ApplyMsg, 0)
	rf.initLeaderVolatileState()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()

	go initTimer(rf)

	return rf
}

func initTimer(rf *Raft) {
	electionWaitTimeMs := ELECTION_MIN_TIME_MS + rand.Int63n(ELECTION_MIN_TIME_MS)
	if rf.timer == nil {
		rf.timer = time.NewTimer(time.Millisecond * time.Duration(electionWaitTimeMs))
		go func() {
			for {
				<-rf.timer.C
				if rf.leader == -1 {
					//广播要求选举
					rf.requestVoteMe()
				} else if rf.state == LEADER {
					//本节点是leader
					//到了广播心跳的时候
					rf.broadcastHeartBeat()
				} else {
					//leader out of date
					//TODO 自己参与选举
					rf.requestVoteMe()
				}
				rf.autoResetTimer()
			}
		}()
	}
}

func (rf *Raft) requestVoteMe() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.switchState(CANDIDATE)
	rf.currentTerm++
	rf.candidateVoteMeCount = 1 //初始投自己
	rf.votedFor = rf.me
	lastLogTerm := 0
	l := len(rf.log)
	if l > 0 {
		lastLogTerm = rf.log[l-1].Term
	}
	if rf.commitIndex > 0 {
		lastLogTerm = rf.log[rf.commitIndex-1].Term
	}
	requestVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.commitIndex,
		LastLogTerm:  lastLogTerm,
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(targetServer int) {
				var reply RequestVoteReply
				ok := rf.peers[targetServer].Call("Raft.RequestVote", requestVoteArgs, &reply)
				if ok {
					rf.countVote(targetServer, reply)
				}
			}(i)
		}
	}
}

func (rf *Raft) countVote(server int, reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.switchState(FOLLOWER)
	}
	if rf.state == CANDIDATE && reply.VoteGranted {
		rf.candidateVoteMeCount++
		if rf.candidateVoteMeCount >= (len(rf.peers)/2)+1 {

			rf.switchState(LEADER)
		}
	}
	rf.persist()
}
