package raft

import (
	"sort"
	"time"
)

type LogEntry struct {
	Term         int
	CommandValid bool
	Command      interface{}
}

// 心跳args
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm
	// align the term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLock(args.Term)
	}

	// 如果 prevLogIndex 大于等于 len(rf.log) 拒绝，follower的log太短
	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = InvalidTerm
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower'log too short",
			args.LeaderId)
		return
	}
	// 如果 prevLogTerm 不符合，拒绝，term不符合
	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, term not match",
			args.LeaderId)
		reply.ConflictTerm = rf.log[args.PrevLogTerm].Term
		reply.ConflictIndex = rf.firstLogFor(args.PrevLogTerm)
		return
	}
	// prevLogIndex 和 prevLogTerm 都满足，需要将args里的log添加到rf里，
	// 打印添加了多少日志， success为true
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	rf.persistLocked()
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, append (%d, %d]",
		args.LeaderId, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	if rf.commmitIndex < args.LeaderCommit {
		LOG(rf.me, rf.currentTerm, DApply, "Follower accept log: (%d,%d]",
			rf.commmitIndex, args.LeaderCommit)
		rf.applyCond.Signal()
		rf.commmitIndex = args.LeaderCommit
	}
	// rf.resetElectionTimerLock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) getMajorityIndexLocked() int {
	// 对matchIndex排序，取中位数，这个中位数就是当前超过半数提交的commitIndex
	tempIndex := make([]int, len(rf.peers))
	copy(tempIndex, rf.matchIndex)
	sort.Ints(sort.IntSlice(tempIndex))
	majorityIdx := (len(rf.peers) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "Match after sort: %v, majority[%d]=%d",
		tempIndex, majorityIdx, tempIndex[majorityIdx])
	return tempIndex[majorityIdx]
}

func (rf *Raft) startReplication(term int) bool {
	replicationToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		// 发送心跳请求
		ok := rf.sendAppendEntries(peer, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 发送心跳请求失败
		if !ok {
			LOG(rf.me, rf.currentTerm, DVote, "->S%d, sendApeendEntries Fail", peer)
			return
		}
		// 根据对方回复的心跳消息，对齐term
		if reply.Term > rf.currentTerm {
			// 对方的term大，我们变为follower， return false
			rf.becomeFollowerLock(reply.Term)
			return
		}
		if rf.contextLostLock(term, Leader) {
			LOG(rf.me, rf.currentTerm, DLog, "->S%d, Context lost, T%d:Leader->T%d:%s",
				peer, term, rf.currentTerm, rf.role)
			return
		}

		// 处理replication的回复
		// 日志复制不成功，在log里从 prevLog 开始往前找，
		// 到 args 的 term 的第一条日志的index 给到 nextIndex[peer]
		// 打印 not match with peer in args.prevLogIdex, try next nextIndex[peer]
		if !reply.Success {
			prevIndex := rf.nextIndex[peer]
			if reply.ConflictTerm == InvalidTerm {
				rf.nextIndex[peer] = reply.ConflictIndex
			} else {
				firstIndex := rf.firstLogFor(reply.ConflictIndex)
				if firstIndex != InvalidIndex {
					rf.nextIndex[peer] = firstIndex
				} else {
					rf.nextIndex[peer] = reply.ConflictIndex
				}
				// 防止无序的reply
				if rf.nextIndex[peer] > prevIndex {
					rf.nextIndex[peer] = prevIndex
				}
			}
			LOG(rf.me, rf.currentTerm, DLog,
				"->S%d not match at %d, try next=%d",
				peer, args.PrevLogIndex, rf.nextIndex[peer])
			return
		}
		// 日志复制成功，更新matchIndex 和nextIndex
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		majorityIndex := rf.getMajorityIndexLocked()
		if majorityIndex > rf.commmitIndex {
			LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit %d->%d",
				rf.commmitIndex, majorityIndex)
			rf.commmitIndex = majorityIndex
			rf.applyCond.Signal()
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 判断上下文
	if rf.contextLostLock(term, Leader) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost Leader [%d] to %s[T%d]", rf.me, rf.role, rf.currentTerm)
		return false
	}
	// 向所有peer发送心跳
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			rf.nextIndex[peer] = len(rf.log)
			rf.matchIndex[peer] = len(rf.log) - 1
			continue
		}
		prevIndex := rf.nextIndex[peer] - 1
		prevTerm := rf.log[prevIndex].Term
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log[prevIndex+1:],
			LeaderCommit: rf.commmitIndex,
		}
		go replicationToPeer(peer, args)
	}
	return true
}

// 每过一个Interval发起一个心跳
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			break
		}
		time.Sleep(replicationInterval)
	}
}
