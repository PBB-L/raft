package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server. 创建一个新的 Raft 服务器

// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry 开始就新的日志条目达成协议

// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader 询问 Raft 的当前任期，以及它是否认为自己是领导者

// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
// 每次向日志提交新条目时，每个 Raft 对等点都应向同一服务器中的服务（或测试者）发送 ApplyMsg。

import (
	//"bytes"
	"sync"
	"sync/atomic"
	"time"

	//"6.824/labgob"
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

type State string

const(
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)


//
// A Go object implementing a single Raft peer. 实现单个 Raft 对等体的 Go 对象
//

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state 锁定以保护对此对等方状态的共享访问
	peers     []*labrpc.ClientEnd // RPC end points of all peers 所有对等方的 RPC 端点
	persister *Persister          // Object to hold this peer's persisted state 保持此对等点的持久状态的对象
	me        int                 // this peer's index into peers[] 此对等点的对等点索引[]
	dead      int32               // set by Kill() 由 Kill() 设置

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state State
	appendEntryCh chan *Entry // 发送 Entry 的 channel
	heartBeat     time.Duration
	electionTime  time.Time

	// state :
	// Persistent state on all servers 所有服务器上的持久状态
	currentTerm int
	votedFor int
	log Log

	// Volatile state on all servers 所有服务器上的易失性状态
	commitIndex int
	lastApplied int

	// Volatile state on leaders 领导者（服务器）上的易失性状态(选举后已经重新初始化)
	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg 	// 发送 ApplyMsg 的channel
	applyCond  *sync.Cond 	//
}

// return currentTerm and whether this server believes it is the leader.
// 返回 currentTerm 以及该服务器是否认为它是领导者。

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage, 将 Raft 的持久状态保存到稳定的存储中，
// where it can later be retrieved after a crash and restart. 崩溃后可以在其中检索它并重新启动。
// see paper's Figure 2 for a description of what should be persistent. 请参阅论文的图 2，了解应该持久化的内容。
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
}


//
// restore previously persisted state. 恢复以前的持久状态
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
// example code to send a RequestVote RPC to a server. 将 RequestVote RPC 发送到服务器的代码
// server is the index of the target server in rf.peers[]. server 是 rf.peers[] 中目标服务器的索引
// expects RPC arguments in args. 期望 args 中的 RPC 参数
// fills in *reply with RPC reply, so caller should
// pass &reply. 用 RPC 回复填写 *reply，所以调用者应该通过 &reply
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers). 传递给 Call() 的参数和回复的类型必须与处理函数中声明的参数的类型相同（包括它们是否是指针）。
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost. labrpc 包模拟了一个有损网络，其中服务器可能无法访问，并且请求和回复可能会丢失。
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.	 Call() 发送请求并等待回复。 如果回复在超时间隔内到达，则 Call() 返回 true； 否则 Call() 返回 false。 因此 Call() 可能暂时不会返回
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply. 错误返回可能由损害的服务器、无法访问的活动服务器、丢失的请求或丢失的回复引起。
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().  如果服务器端的处理函数没有返回，则 Call() 保证返回（可能在延迟之后）*except*。 因此，无需围绕 Call() 实现您自己的超时。
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself. 如果您无法让 RPC 工作，请检查您是否已将通过 RPC 传递的结构中的所有字段名称大写，并且调用者使用 & 传递回复结构的地址，而不是结构本身。
//


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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test, 	每次测试后，测试人员不会停止 Raft 创建的 goroutines，
// but it does call the Kill() method. your code can use killed() to 	但它确实调用了 Kill() 方法。
// check whether Kill() has been called. the use of atomic avoids the 	您的代码可以使用killed() 来检查是否调用了Kill()。
// need for a lock.														atomic 的使用避免了对锁的需要。
//
// the issue is that long-running goroutines use memory and may chew 	长时间运行的 goroutine 会占用内存并且可能会占用 CPU 时间，
// up CPU time, perhaps causing later tests to fail and generating		可能会导致以后的测试失败并产生令人困惑的调试输出。
// confusing debug output. any goroutine with a long-running loop		任何具有长时间运行循环的 goroutine 都应该调用 killed() 来检查它是否应该停止。
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently. 如果此对等点最近没有收到心跳，则ticker goroutine 开始新的选举。
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		time.Sleep(rf.heartBeat)
		rf.mu.Lock()
		if rf.state == Leader {
			for serverId := range rf.peers{
				if serverId != rf.me {
					rf.appendEntries(true)
				}
			}
		}
		if time.Now().After(rf.electionTime){ //  判断 当前本地的时间  是否在 rf.electionTime 之后
			// 到rf.electionTime 就开始领导者选举
			rf.leaderElection()
		}
		rf.mu.Unlock()
	}
}

//
// the service or tester wants to create a Raft server. the ports  		服务或测试人员想要创建一个 Raft 服务器。 所有 Raft 服务器（包括这个）的端口都在 peers[] 中。
// of all the Raft servers (including this one) are in peers[]. this 	此服务器的端口是 peers[me]。 所有服务器的 peers[] 数组都具有相同的顺序。
// server's port is peers[me]. all the servers' peers[] arrays			persister 是此服务器保存其持久状态的地方，并且最初还保存最近保存的状态（如果有）。
// have the same order. persister is a place for this server to			applyCh 是测试人员或服务期望 Raft 发送 ApplyMsg 消息的通道。
// save its persistent state, and also initially holds the most			Make() 必须快速返回，因此它应该为任何长时间运行的工作启动 goroutine。
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// 创建一个后台goroutine：通过在一段时间没有收到其他对等方的消息时发送RequestVote RPC来定期启动领导者选举
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).



	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
