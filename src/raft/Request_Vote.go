package raft

import "sync"

// example RequestVote RPC arguments structure.  RequestVote RPC 参数结构
// field names must start with capital letters! 字段名称必须以大写字母开头！
//

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int   		 // 候选者任期号
	CandidateID int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure. RequestVote RPC 回复结构
// field names must start with capital letters!
//

type RequestVoteReply struct {
	// Your data here (2A).
	Term int		// 服务器当前任期号
	VoteGranted bool
}

// example RequestVote RPC handler. RequestVote RPC 处理程序
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm{
		// receiver's accomplish 1
		reply.VoteGranted = false
		return
	}

	var myLastLogIndex = rf.log.lastLog().Index
	var myLastLogTerm = rf.log.lastLog().Term

	if (rf.votedFor == args.CandidateID || rf.votedFor == int(0) ) && (myLastLogIndex == args.LastLogIndex && myLastLogTerm == args.LastLogTerm){
		// receiver's accomplish 2
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	}else {
		reply.VoteGranted = false
	}
}

// 候选者 发送给 其他服务器的 RequestVote RPC
func (rf *Raft) candidateRequestVote(serverId int,args *RequestVoteArgs,voteCounter *int,becomeLeader *sync.Once) {
	// 判断 RequestVote RPC 是否发送给 其他服务器
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverId,args,&reply)
	if !ok {
		return
	}

	//

}

// 判断发送给 server服务器 的 RequestVote 是否成功
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
