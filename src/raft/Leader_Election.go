package raft

import (
	"math/rand"
	"sync"
	"time"
)

func (rf *Raft) leaderElection() {
	// 重置选举定时器
	rf.resetElectionTime()

	// 更新自己的任期号并转换到候选人状态，给自己投票
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me

	voteCounter := 1


	// 将自己的信息放入到 RequestVote RPC中
	term := rf.currentTerm
	candidateId := rf.me
	lastLogIndex := rf.log.lastLog().Index
	lastLogTerm := rf.log.lastLog().Term

	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  candidateId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// 向集群中的其他服务器节点发送 RequestVote RPC
	var becomeLeader sync.Once
	for serverId := range rf.peers {
		if serverId != rf.me {
			go rf.candidateRequestVote(serverId,&args,&voteCounter,&becomeLeader)
		}
	}
}

func (rf *Raft) resetElectionTime() {
	t := time.Now()
	electionTimeout := time.Duration(150 + rand.Intn(150)) * time.Millisecond // 选举超时时间 在区间[150,300]ms 内随机选取
	rf.electionTime = t.Add(electionTimeout)
}
