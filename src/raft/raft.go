package raft

/*
	测试：VERBOSE=0 go test -run PartA | tee out.txt
*/
//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details
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

	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"course/labrpc"
)

const (
	// 选举超时时间
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 400 * time.Millisecond

	// 心跳发送间隔
	replicationInterval time.Duration = 80 * time.Millisecond
)

const (
	InvalidTerm  int = 0
	InvalidIndex int = 0
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	role Role

	nextIndex  []int
	matchIndex []int

	commmitIndex int
	lastApplied  int
	applych      chan ApplyMsg
	applyCond    *sync.Cond

	electionStart   time.Time
	electionTimeout time.Duration
}

func (rf *Raft) becomeFollowerLock(term int) {
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DError, "Can't become Follower")
		return
	} else if term > rf.currentTerm {
		// 比我的term大，说明出现了新的term，需要在这个任期重新投票
		rf.votedFor = -1
	}
	LOG(rf.me, rf.currentTerm, DLog,
		"%s->Follower, For T%d->T%d", rf.role, rf.currentTerm, term)
	rf.role = Follower
	rf.currentTerm = term
	if rf.currentTerm != term {
		// term 不变，它的voteFor一定不变
		rf.persistLocked()
	}
}

func (rf *Raft) becomeCandidateLock() {
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DError, "-> role is %s, Can't become Candidate", rf.role)
		return
	}
	LOG(rf.me, rf.currentTerm, DVote, "-> become Candidate")
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.persistLocked()
}

func (rf *Raft) becomeLeader() {
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Can't become Leader")
		return
	}
	LOG(rf.me, rf.currentTerm, DLog, "become Leader T%d", rf.currentTerm)
	rf.role = Leader
	// 重置 nextIndex 和 matchIndex
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = len(rf.log)
		rf.matchIndex[peer] = 0
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (PartA).
	return rf.currentTerm, rf.role == Leader
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

}

func (rf *Raft) firstLogFor(term int) int {
	for idx, entry := range rf.log {
		if entry.Term == term {
			return idx
		} else if entry.Term > term {
			break
		}
	}
	return InvalidTerm
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
	// Your code here (PartB).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return 0, 0, false
	}
	rf.log = append(rf.log, LogEntry{
		Term:         rf.currentTerm,
		CommandValid: true,
		Command:      command,
	})
	LOG(rf.me, rf.currentTerm, DLeader, "Leader accept log [%d]T%d",
		len(rf.log)-1, rf.currentTerm)
	rf.persistLocked()
	return len(rf.log) - 1, rf.currentTerm, true
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

// 上下文包括 role 和 term 是否发生变化
func (rf *Raft) contextLostLock(term int, role Role) bool {
	return !(rf.currentTerm == term && rf.role == role)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (PartA, PartB, PartC).
	rf.currentTerm = 1
	rf.role = Follower
	rf.votedFor = -1

	// 开头有一个空日志
	rf.log = append(rf.log, LogEntry{Term: InvalidTerm})

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.lastApplied = 0
	rf.commmitIndex = 0
	rf.applych = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.applicationTicker()
	return rf
}
