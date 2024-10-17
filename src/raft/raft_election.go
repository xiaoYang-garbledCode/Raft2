package raft

import (
	"math/rand"
	"time"
)

// 重置超时时间, 防止多个server同时超时发起选举，这个超时时间需要随机
func (rf *Raft) resetElectionTimerLock() {
	rf.electionStart = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	// 随机时间是 electionTimeoutMih + rand % randRange
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

// 判断这个server是否超时
func (rf *Raft) isElectionTimeoutLock() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	GrantedVote bool
	Term        int
}

func (rf *Raft) isMoreUpdatedLock(lastLogIndex int, lastLogTerm int) bool {
	rfLastLogTerm := rf.log[len(rf.log)-1].Term
	if rfLastLogTerm == lastLogTerm {
		// lastTerm 相同 谁的index大，谁新
		return lastLogIndex >= len(rf.log)-1
	} else {
		// lastTerm 不同，谁的term大，谁新
		return lastLogTerm >= rfLastLogTerm
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).
	// server 收到对方的投票请求
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.GrantedVote = false
	reply.Term = rf.currentTerm
	// 对齐term
	// term比我小，直接拒绝
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject vote, Higher term, T%d>%d",
			args.CandidateId, rf.currentTerm, args.Term)
		return
	}
	// term比我大，我变成Follower
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLock(args.Term)
	}
	// 判断我是否投过票
	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject vote, Already vote to S%d",
			args.CandidateId, rf.votedFor)
		return
	}
	// 只有对方的日志和我们的一样新或 more 新的时候才能给它投票。
	if !rf.isMoreUpdatedLock(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DVote,
			"->S%d, Reject, Candidate is less update", args.CandidateId)
		return
	}
	// 给对方投票，重置超时时间，打印
	rf.votedFor = args.CandidateId
	reply.GrantedVote = true
	rf.resetElectionTimerLock()
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Vote granted", args.CandidateId)
	rf.persistLocked()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection(term int) {
	vote := 0
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		// 准备一个reply 用于接收peer对投票请求的回应
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)
		// 处理投票回应
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 发送请求失败
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "Ask vote from S%d, Lost or Error", peer)
			return
		}
		// 对齐term
		if rf.currentTerm < reply.Term {
			rf.becomeFollowerLock(reply.Term)
			return
		}
		// 判断上下文是否变化, 发起请求时这个rf的role需要是Candidate
		if rf.contextLostLock(term, Candidate) {
			// 上下文丢失
			LOG(rf.me, rf.currentTerm, DVote, "Lost Context, abort voteRequest from S%d", peer)
			return
		}
		// term符合，且上下文不丢失，计算投票数，判断能否成为leader
		if reply.GrantedVote {
			vote++
			if vote > len(rf.peers)/2 {
				// 成为leader
				rf.becomeLeader()
				// 需要开启一个协程维护心跳
				go rf.replicationTicker(term)
			}
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 判断上下文
	if rf.contextLostLock(term, Candidate) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost Candidate to%s, abort voteRequest", rf.role)
		return
	}
	// 向所有peer发送选举请求
	for peer := 0; peer < len(rf.peers); peer++ {
		if rf.me == peer {
			vote++
			continue
		}
		// 组装请求参数
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log) - 1,
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
		// 发起请求
		go askVoteFromPeer(peer, &args)
	}
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {
		// Your code here (PartA)
		// 超时且不是leader，就需要开启选举
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeoutLock() {
			// 变为Candidate
			rf.becomeCandidateLock()
			// 起一个协程，去开启选举流程
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()
		// 每个机器的睡眠时间
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
