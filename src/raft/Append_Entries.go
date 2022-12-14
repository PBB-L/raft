package raft

// AppendEntries PRC

type AppendEntriesArgs struct {
	Term int
	PrevLogIndex int
	PrevLogTerm int
	LeaderId int
	LeaderCommit int
	Entries Log
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs,reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		// receiver's accomplish 1
		reply.Success = false
		return
	}

	for id := 0; id < rf.log.len(); id++ {
		// receiver's accomplish 2
		if rf.log.at(id).Index == args.PrevLogIndex && rf.log.at(id).Term == args.PrevLogTerm {
			break
		}

		if id == rf.log.len() {
			reply.Success = false
		}
	}

	for id := 0;id < rf.log.len(); id++ {
		// receiver's accomplish 3
		if rf.log.at(id).Index == args.Entries.at(id).Index &&  rf.log.at(id).Term != args.Entries.at(id).Term {
			rf.log.truncate(id)
			break
		}
	}

	for id := rf.log.len(); id < args.Entries.len(); id++{
		// receiver's accomplish 4
		rf.log.append(args.Entries.Entries[id])
	}

	if rf.commitIndex < args.LeaderCommit {
		// receiver's accomplish 5
		var min int
		if rf.commitIndex < args.LeaderCommit{
			min = rf.commitIndex
		}else{
			min = args.LeaderCommit
		}
		rf.commitIndex = min
	}
}

// 成为领导者发送的 空心跳包
func (rf *Raft) appendEntries(isLeader bool) {
	// Ruler for Servers : Leaders 1
	for serverId := range rf.peers {
		if serverId == rf.me {
			rf.resetElectionTime()
			continue
		}
		// Ruler for Servers : Leaders 3
		if rf.log.lastLog().Index >= rf.nextIndex[serverId] {

		}
	}
}

// 5.3
func (rf *Raft) leaderSendEntries(serverId int,args *AppendEntriesArgs){

}

func (rf *Raft) leaderCommitRule() {
	// Ruler for Servers : Leaders 4
}