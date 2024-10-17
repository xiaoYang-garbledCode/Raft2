package raft

func (rf *Raft) applicationTicker() {
	for !rf.killed() {
		// 先把 lastApplied 到 commitIndex 直接的log添加到entries，然后释放锁
		rf.mu.Lock()
		rf.applyCond.Wait()
		entries := make([]LogEntry, 0)
		for i := rf.lastApplied + 1; i <= rf.commmitIndex; i++ {
			entries = append(entries, rf.log[i])
		}
		rf.mu.Unlock()
		// 遍历 entries，将log apply到applymsg中
		for i, entry := range entries {
			rf.applych <- ApplyMsg{
				CommandValid: entry.CommandValid,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied + i + 1,
			}
		}
		rf.mu.Lock()
		LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]",
			rf.lastApplied+1, rf.commmitIndex)
		rf.lastApplied = rf.commmitIndex
		rf.mu.Unlock()
	}
}
