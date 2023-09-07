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
	"fmt"
	// "log"

	// "fmt"
	// "html/template"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//

const (
	follower  = 0
	candidate = 1
	leader    = 2
)

type LogEntry struct {
	Command interface{}
	Term    int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	nPeer         int
	term          int
	state         int
	voted         bool
	heartsbeatsCh chan struct{}
	stoptickerCh  chan struct{}

	applyCh     chan ApplyMsg
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	logCond     sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.term
	isleader = rf.state == leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.voted)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var pTerm int
	var pVoted bool
	var pLog []LogEntry
	if d.Decode(&pTerm) != nil ||
		d.Decode(&pVoted) != nil ||
		d.Decode(&pLog) != nil {
		fmt.Printf("[%d] readPersist error\n", rf.me)
		panic("")
	} else {
		rf.term = pTerm
		rf.voted = pVoted
		rf.log = pLog
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.CandidateId == rf.me {
		panic("send requestvote RPC to self")
	}
	if args.Term > rf.term {
		rf.term = args.Term
		rf.state = follower
		reply.Term = rf.term
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {
			rf.voted = true
			reply.VoteGranted = true
			rf.heartsbeatsCh <- struct{}{}
			DPrintf("[%d] vote for %d. term: %d\n", rf.me, args.CandidateId, rf.term)
		} else {
			rf.voted = false
			reply.VoteGranted = false
		}
	} else if args.Term == rf.term {
		if !rf.voted && (args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)) {
			reply.VoteGranted = true
			rf.voted = true
			rf.heartsbeatsCh <- struct{}{}
			DPrintf("[%d] vote for %d. term: %d\n", rf.me, args.CandidateId, rf.term)
		} else {
			reply.VoteGranted = false
		}
		reply.Term = rf.term
	} else {
		reply.Term = rf.term
		reply.VoteGranted = false
	}
	rf.persist()
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// AppendEntries RPC.
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entrys       []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	Xindex  int
	Xlen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == leader && rf.term == args.Term {
		DPrintf("[%d] double leader: %d. term: %d\n", rf.me, args.LeaderId, args.Term)
		panic("")
	}
	if args.Term < rf.term {
		reply.Term = rf.term
		return
	}
	rf.heartsbeatsCh <- struct{}{}
	if args.Term > rf.term {
		rf.term = args.Term
		rf.voted = false
		rf.state = follower
		rf.persist()
	}
	if args.Term == rf.term && rf.state == candidate {
		rf.state = follower
	}
	reply.Term = rf.term

	if args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		reply.Success = true
		if args.Entrys == nil {
			rf.commitIndex = max(rf.commitIndex, min(min(args.PrevLogIndex, args.LeaderCommit), len(rf.log)-1))
			rf.applyLog()
			return
		}
		if args.PrevLogIndex == len(rf.log)-1 {
			rf.log = append(rf.log, args.Entrys...)
		} else if args.PrevLogIndex < len(rf.log)-1 {
			rf.log = rf.log[:args.PrevLogIndex+1]
			rf.log = append(rf.log, args.Entrys...)
		}
		rf.persist()
		rf.commitIndex = max(rf.commitIndex, min(args.LeaderCommit, len(rf.log)-1))
		rf.applyLog()
	} else {
		reply.Success = false
		if args.PrevLogIndex < len(rf.log) {
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			reply.Xindex = rf.findFirstLog(reply.XTerm, args.PrevLogIndex)
		} else {
			reply.Xlen = len(rf.log)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term != rf.term {
		return
	}
	if reply.Term > rf.term {
		DPrintf("[%d] term is lower than follower: %d\n", rf.me, rf.term)
		rf.term = reply.Term
		rf.voted = false
		rf.state = follower
		rf.heartsbeatsCh <- struct{}{}
		rf.persist()
		return
	}
	if reply.Success {
		rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex)
		DPrintf("matchIndex[%d] updated: %d", server, rf.matchIndex[server])
		if rf.matchIndex[server] > rf.commitIndex && rf.checkCommit(rf.matchIndex[server]) {
			rf.commitIndex = rf.matchIndex[server]
			rf.applyLog()
		}
		if args.Entrys == nil {
			if rf.nextIndex[server] != len(rf.log) {
				rf.logCond.Broadcast()
			}
			return
		}

		if args.PrevLogIndex == rf.nextIndex[server]-1 && rf.nextIndex[server] < len(rf.log) {
			rf.nextIndex[server] += len(args.Entrys)
		}
		DPrintf("[%d] send AppendEntries Success to %d. idx: %d term: %d nextIndex: %d\n", rf.me, server, args.PrevLogIndex, args.PrevLogTerm, rf.nextIndex[server])
	} else {
		if args.PrevLogIndex == rf.nextIndex[server]-1 {
			// rf.nextIndex[server]--
			if reply.XTerm == 0 && reply.Xindex == 0 {
				rf.nextIndex[server] = reply.Xlen
			} else if rf.log[reply.Xindex].Term == reply.XTerm {
				rf.nextIndex[server] = rf.findLastLog(reply.XTerm, reply.Xindex) + 1
			} else {
				rf.nextIndex[server] = reply.Xindex
			}
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			if rf.nextIndex[server] == 0 {
				panic("first log")
			}
			DPrintf("[%d] send AppendEntries Failed. idx: %d term: %d\n", rf.me, args.PrevLogIndex, args.PrevLogTerm)
		}
	}
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (rf *Raft) findFirstLog(term, lastIndex int) int {
	l := 0
	r := lastIndex
	for l < r {
		mid := (l + r) >> 1
		if rf.log[mid].Term < term {
			l = mid + 1
		} else {
			r = mid
		}
	}
	return l
}

func (rf *Raft) findLastLog(term, firstIndex int) int {
	l := firstIndex
	r := len(rf.log)
	for l < r {
		mid := (l + r + 1) >> 1
		if rf.log[mid].Term <= term {
			l = mid
		} else {
			r = mid - 1
		}
	}
	return l
}

func (rf *Raft) checkCommit(index int) bool {
	// if rf.log[index].Term != rf.term {
	// 	return false
	// }
	num := 0
	for _, idx := range rf.matchIndex {
		if idx >= index {
			num++
		}
	}
	return num > (rf.nPeer >> 1)
}

func (rf *Raft) applyLog() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied}
		DPrintf("[%d] log %d applied to state machine\n", rf.me, rf.lastApplied)
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.term
	isLeader := rf.state == leader

	// Your code here (2B).
	if rf.state == leader {
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{command, rf.term})
		rf.matchIndex[rf.me] = len(rf.log) - 1
		rf.logCond.Broadcast()
		DPrintf("[%d] leader log add one: %d\n", rf.me, len(rf.log)-1)
		rf.persist()
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) leaderfunc(term int, stopelect chan struct{}) {
	var terminate int32 = 0
	rf.mu.Lock()
	rf.stoptickerCh <- struct{}{}
	rf.logCond = *sync.NewCond(&rf.mu)
	for i := 0; i < rf.nPeer; i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.mu.Unlock()
	for i := 0; i < rf.nPeer; i++ {
		if i != rf.me {
			// heartbeat goroutine
			go func(server int) {
				for !rf.killed() && atomic.LoadInt32(&terminate) == 0 {
					rf.mu.Lock()
					args := AppendEntriesArgs{term, rf.me, rf.nextIndex[server] - 1, rf.log[rf.nextIndex[server]-1].Term, nil, rf.commitIndex}
					rf.mu.Unlock()
					reply := AppendEntriesReply{}
					go rf.sendAppendEntries(server, &args, &reply)
					time.Sleep(100 * time.Millisecond)
				}
			}(i)
			// append log goroutine
			go func(server int) {
				for !rf.killed() && atomic.LoadInt32(&terminate) == 0 {
					rf.logCond.L.Lock()
					for !rf.killed() && atomic.LoadInt32(&terminate) == 0 && rf.nextIndex[server] >= len(rf.log) {
						DPrintf("nextIndex[%d]: %d, waiting\n", server, rf.nextIndex[server])
						rf.logCond.Wait()
					}
					rf.logCond.L.Unlock()
					for !rf.killed() && atomic.LoadInt32(&terminate) == 0 {
						rf.mu.Lock()
						args := AppendEntriesArgs{term, rf.me, rf.nextIndex[server] - 1, rf.log[rf.nextIndex[server]-1].Term, rf.log[rf.nextIndex[server]:], rf.commitIndex}
						rf.mu.Unlock()
						reply := AppendEntriesReply{}
						rf.sendAppendEntries(server, &args, &reply)
						rf.mu.Lock()
						if rf.nextIndex[server] >= len(rf.log) {
							rf.mu.Unlock()
							break
						} else {
							rf.mu.Unlock()
						}
					}
				}
			}(i)
		}
	}
	<-stopelect
	atomic.StoreInt32(&terminate, 1)
}

func (rf *Raft) candidatefunc(term int, stopelect chan struct{}) {
	var terminate, votes int32 = 0, 1
	exitch := make(chan struct{}, rf.nPeer)
	electedch := make(chan struct{}, rf.nPeer)
	for i := 0; i < rf.nPeer; i++ {
		if i != rf.me {
			go func(server int) {
				for !rf.killed() && atomic.LoadInt32(&terminate) == 0 {
					rf.mu.Lock()
					args := RequestVoteArgs{term, rf.me, len(rf.log) - 1, rf.log[len(rf.log)-1].Term}
					rf.mu.Unlock()
					reply := RequestVoteReply{}
					if rf.sendRequestVote(server, &args, &reply) {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if reply.Term > term {
							DPrintf("[%d] term is lower than follower: %d\n", rf.me, term)
							atomic.StoreInt32(&terminate, 1)
							if rf.term == term {
								rf.term = reply.Term
								rf.voted = false
								rf.state = follower
								rf.persist()
							}
							if atomic.LoadInt32(&terminate) == 0 {
								exitch <- struct{}{}
							}
						} else if reply.VoteGranted {
							DPrintf("[%d] recieve vote from %d\n", rf.me, server)
							atomic.AddInt32(&votes, 1)
							if atomic.LoadInt32(&terminate) == 0 && int(atomic.LoadInt32(&votes)) > (rf.nPeer>>1) {
								electedch <- struct{}{}
							}
						}
						return
					}
				}
			}(i)
		}
	}
	select {
	case <-electedch:
		atomic.StoreInt32(&terminate, 1)
		rf.mu.Lock()
		if rf.term == term {
			rf.state = leader
			DPrintf("[%d] become leader\n", rf.me)
			go rf.leaderfunc(term, stopelect) // go leader function
		}
		rf.mu.Unlock()
	case <-exitch:
		atomic.StoreInt32(&terminate, 1)
	case <-stopelect:
		atomic.StoreInt32(&terminate, 1)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	var stopelect chan struct{}
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.heartsbeatsCh:
			if stopelect != nil {
				close(stopelect)
				stopelect = nil
			}
		case <-time.After(time.Duration(250+(rand.Int()%150)) * time.Millisecond):
			rf.mu.Lock()
			DPrintf("[%d] expired election time. term: %d\n", rf.me, rf.term)
			rf.state = candidate
			rf.term++
			rf.voted = true
			rf.persist()
			if stopelect != nil {
				close(stopelect)
			}
			stopelect = make(chan struct{})
			go rf.candidatefunc(rf.term, stopelect)
			rf.mu.Unlock()
		case <-rf.stoptickerCh:
			DPrintf("[%d] ticker stopped\n", rf.me)
			<-rf.heartsbeatsCh
			if stopelect != nil {
				close(stopelect)
				stopelect = nil
			}

			DPrintf("[%d] ticker resumed\n", rf.me)
		}
	}
	if stopelect != nil {
		close(stopelect)
		stopelect = nil
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.nPeer = len(rf.peers)
	rf.heartsbeatsCh = make(chan struct{})
	rf.stoptickerCh = make(chan struct{})
	rf.voted = false
	rf.state = follower
	rf.term = 0
	rf.applyCh = applyCh
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{nil, 0}
	rf.nextIndex = make([]int, rf.nPeer)
	rf.matchIndex = make([]int, rf.nPeer)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
