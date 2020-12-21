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
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

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
}

const (
	FOLLOWER = 0
	CANDIDATE = 1
	LEADER = 2
)

//日志log的结构体，包含日志的Term和具体Command
type LogEntry struct {
	Term int
	Command interface{}
}
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
	//Persistent state on all servers:
	currentTerm int
	votedFor int
	log []LogEntry
	//Volatile state on all servers:
	commitIndex int
	lastApplied int
	//Volatile state on leaders:
	nextIndex []int
	matchIndex []int
	//other
	state int //服务器状态
	votes int //获得的票数
	heartBeat chan interface{} //传送心跳的通道
	voteMajority chan interface{} //如果获得大多数选票，则从这个通道告知
}

//根据日志index获取Term
func (rf *Raft) GetTerm(index int) int{
	if index>=len(rf.log) || index<0{
		return 0
	}
	return rf.log[index].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	if rf.state == LEADER{
		isleader = true
	}else{
		isleader = false
	}
	return term, isleader
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
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}


//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	//如果Candidate的Term小于等于自己的Term则不投票
	//Reply false if term < currentTerm
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.persist()
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	//If votedFor is null or candidateId, and candidate’s log is at
	//least as up-to-date as receiver’s log, grant vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm>rf.GetTerm(len(rf.log)-1) ||
		(args.LastLogTerm == rf.GetTerm(len(rf.log)-1) && args.LastLogIndex>= len(rf.log)-1)){
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.state = FOLLOWER
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		//fmt.Printf("Term %d:%d投给%d\n",rf.currentTerm,rf.me,rf.votedFor)
	}else{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	rf.persist()
	rf.mu.Unlock()
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
	rf.mu.Lock()
	if ok{
		//如果不是在当前发送Term竞选则无效
		if rf.state != CANDIDATE || args.Term != rf.currentTerm{
			rf.mu.Unlock()
			return ok
		}
		//如果有一个更新的Term回复
		if reply.Term > rf.currentTerm{
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
			return ok
		}
		//收到选票
		if reply.VoteGranted{
			rf.votes++
			//选票数超过总人数一半，赢得选举
			if rf.votes > len(rf.peers)/2{
				rf.votes = -1 //防止有延迟的投票导致此机器又更新为Leader
				rf.voteMajority <- true
			}
		}
	}
	rf.persist()
	rf.mu.Unlock()
	return ok
}

//给所有人发送sendRequestVote
func (rf *Raft) sendRequestVoteToAll(){
	for i:=0;i<len(rf.peers);i++{
		if i==rf.me{
			continue
		}
		args:=RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log)-1,
			LastLogTerm:  rf.GetTerm(len(rf.log)-1),
		}
		var reply RequestVoteReply
		go rf.sendRequestVote(i,args,&reply)
	}
}

//
// example AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	// Your data here.
	Term int
	LeaderId int
	PreLogIndex int
	PreLogTerm int
	Entries []LogEntry
	LeaderCommit int
	Phase int //标识第几阶段的提交（0表示心跳，1表示第一阶段，2表示第二阶段）
}

//
// example AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	// Your data here.
	Term int
	Success bool
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm{
		reply.Success = false
		rf.persist()
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	//如果是心跳
	if len(args.Entries)==0 {
		reply.Success = true
		rf.heartBeat <- true //传送心跳
		rf.persist()
		rf.mu.Unlock()
		return
	}
	//fmt.Printf("%d收到指令preIndex:%d,command:",rf.me,args.PreLogIndex)
	//for i:=0;i<len(args.Entries);i++{
	//	fmt.Println(args.Entries[i].Command)
	//}
	//如果本地的日志数量还不够PreLogIndex
	if len(rf.log)-1<args.PreLogIndex{
		reply.Success = false
		rf.persist()
		rf.mu.Unlock()
		return
	}
	//如果前一条日志的Term和本地最新位置的日志Term相等返回true
	if args.PreLogIndex == rf.commitIndex && args.PreLogTerm == rf.GetTerm(rf.commitIndex) {
		reply.Success = true
		if args.Phase == 1 {
			// If an existing entry conflicts with a new one (same index
			//but different terms), delete the existing entry and all that
			//follow i
			rf.log = rf.log[:args.PreLogIndex+1]
			//Append any new entries not already in the log
			rf.log = append(rf.log, args.Entries...) //取log的前面PreLogIndex+1个元素与要添加的日志拼起来
			//fmt.Println(rf.log)
		}else if args.Phase == 2{
			// If leaderCommit > commitIndex, set commitIndex =
			//min(leaderCommit, index of last new entry)
			if args.LeaderCommit > rf.commitIndex{
				lastIndex := len(rf.log) - 1
				if args.LeaderCommit < lastIndex{
					rf.commitIndex = args.LeaderCommit
				}else{
					rf.commitIndex = lastIndex
				}
			}
		}
	}else{
		// Reply false if log doesn’t contain an entry at prevLogIndex
		//whose term matches prevLogTerm
		reply.Success = false
	}
	rf.persist()
	rf.mu.Unlock()
	return
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	if ok{
		//如果不是leader或者term不对直接返回
		if rf.state != LEADER || args.Term != rf.currentTerm{
			rf.persist()
			rf.mu.Unlock()
			return ok
		}
		//如果有一个更新的Term回复
		if reply.Term > rf.currentTerm{
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
		}
		//If successful: update nextIndex and matchIndex for follower
		if reply.Success && args.Phase == 1 {
			rf.nextIndex[server] = len(rf.log)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		} else if reply.Success && args.Phase == 2{
			//阶段2
		} else if reply.Success && args.Phase == 0{
			//心跳heartbeat
		} else {
			//If AppendEntries fails because of log inconsistency:
			//decrement nextIndex and retry
			//rf.nextIndex[server] = len(rf.log)
			rf.nextIndex[server]--
			args.PreLogIndex--
			if args.PreLogIndex < 0 {
				//args.PreLogTerm = 0
			} else {
				//fmt.Printf("%d的preindex改为%d\n", server, args.PreLogIndex)
				args.PreLogTerm = rf.log[args.PreLogIndex].Term
				args.Entries = append([]LogEntry{rf.log[args.PreLogIndex+1]}, args.Entries...) //按日志的Index顺序插入
				rf.sendAppendEntries(server, args, reply)
			}
		}
	}
	rf.persist()
	rf.mu.Unlock()
	return ok
}

//参数isHeartBeat表示此AppendEntries是否为心跳
func (rf *Raft) sendAppendEntriesToAll(isHeartBeat bool){
	var n int
	var preLogIndex int
	var preLogTerm int
	rf.mu.Lock()
	if isHeartBeat{
		for i:=0;i<len(rf.peers);i++{
			if i==rf.me || rf.state != LEADER{
				continue
			}
			//如果是心跳则传一个空的log
			var logs []LogEntry
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PreLogIndex:  preLogIndex,
				PreLogTerm:   preLogTerm,
				Entries:      logs,
				LeaderCommit: rf.commitIndex,
				Phase:        0,
			}
			var reply AppendEntriesReply
			go rf.sendAppendEntries(i,args,&reply)
		}
		rf.mu.Unlock()
		return
	}

	//Phase1
	//fmt.Println("Phase1...")
	n = rf.commitIndex+1 //本次要提交log的index
	if n>len(rf.log)-1 {
		rf.persist()
		rf.mu.Unlock()
		return
	}
	preLogIndex = n-1
	preLogTerm = rf.GetTerm(preLogIndex)
	for i:=0;i<len(rf.peers);i++{
		if i==rf.me || rf.state != LEADER{
			continue
		}
		rf.nextIndex[i] = n
		var logs []LogEntry
		logs = append(logs, rf.log[n])
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PreLogIndex:  preLogIndex,
			PreLogTerm:   preLogTerm,
			Entries:      logs,
			LeaderCommit: rf.commitIndex,
			Phase:        1,
		}
		var reply AppendEntriesReply
		go rf.sendAppendEntries(i,args,&reply)
	}
	rf.persist()
	rf.mu.Unlock()

	//Phase2
	time.Sleep(5 * time.Millisecond)
	//fmt.Println("Phase2...")
	rf.mu.Lock()
	N := rf.commitIndex + 1
	sum := 1 //同意提交日志的server数
	for i:=0;i<len(rf.peers);i++{
		if i != rf.me && rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
			sum++
		}
	}
	//fmt.Printf("sum=%d\n",sum)
	//if majority
	if sum > len(rf.peers)/2{
		//If there exists an N such that N > commitIndex, a majority
		//of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		//set commitIndex = N
		rf.commitIndex = N
		for i:=0;i<len(rf.peers);i++{
			if i==rf.me || rf.state != LEADER{
				continue
			}
			rf.nextIndex[i] = n
			var logs []LogEntry
			logs = append(logs, rf.log[n])
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PreLogIndex:  preLogIndex,
				PreLogTerm:   preLogTerm,
				Entries:      logs,
				LeaderCommit: rf.commitIndex,
				Phase:        2,
			}
			var reply AppendEntriesReply
			go rf.sendAppendEntries(i,args,&reply)
		}
	}
	rf.persist()
	rf.mu.Unlock()
	return
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
	var isLeader bool
	index := -1
	term := rf.currentTerm
	if rf.state == LEADER{
		isLeader = true
		logEntryItem := LogEntry{
			Term:    term,
			Command: command,
		}
		index = rf.commitIndex + 1
		//index = len(rf.log)
		rf.log = rf.log[:index]
		//If command received from client: append entry to local log,
		//respond after entry applied to state machine
		rf.log = append(rf.log,logEntryItem) //将指令添加到log切片
		rf.persist()
		//fmt.Printf("\n%d添加指令index=%d,command:",rf.me,index)
		//fmt.Println(command)
		go rf.sendAppendEntriesToAll(false) //通知其他机器更新log
	}else{
		isLeader = false
	}
	rf.mu.Unlock()
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
	//fmt.Printf("%d挂了\n",rf.me)
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
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{
		Term:    0,
		Command: 0,
	})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.heartBeat = make(chan interface{})
	rf.voteMajority = make(chan interface{})
	rand.Seed(time.Now().UnixNano())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//fmt.Printf("%d起了，其log为",rf.me)
	//fmt.Println(rf.log)

	//create a background goroutine to apply commands
	go func() {
		for{
			//If commitIndex > lastApplied: increment lastApplied, apply
			//log[lastApplied] to state machine
			rf.mu.Lock()
			if rf.commitIndex < len(rf.log) &&rf.commitIndex>rf.lastApplied{
				rf.lastApplied++
				applyCh <- ApplyMsg{
					Index:   rf.lastApplied,
					Command: rf.log[rf.lastApplied].Command,
				}
				//fmt.Printf("%dapply指令",rf.me)
				//fmt.Println(rf.log[rf.lastApplied].Command)
				//fmt.Println(rf.log)
			}
			rf.mu.Unlock()
		}
	}()

	//create a background goroutine that starts an election
	//by sending out RequestVote RPC when it hasn’t heard from
	//another peer for a while
	go func() {
		for{
			switch rf.state {
			case FOLLOWER:
				select{
				//如果在100-500ms内未收到心跳则start new election
				//If election timeout elapses without receiving AppendEntries
				//RPC from current leader or granting vote to candidate:
				//convert to candidate
				case <- rf.heartBeat:
					//fmt.Printf("%d收到心跳\n", rf.me)
				case <-time.After(time.Millisecond*time.Duration(rand.Int63n(400)+100)):
					//fmt.Printf("%d发起选举\n", rf.me)
					rf.state = CANDIDATE
				}
			case CANDIDATE:
				//• Increment currentTerm
				//• Vote for self
				//• Reset election timer
				//• Send RequestVote RPCs to all other servers
				rf.mu.Lock()
				rf.currentTerm++
				rf.votes = 1
				rf.votedFor = me
				rf.mu.Unlock()
				go rf.sendRequestVoteToAll()
				select {
				//如果收到大多数选票则转换成leader
				//If votes received from majority of servers: become leader
				case <- rf.voteMajority:
					//fmt.Printf("Term %d:%d成为leader\n",rf.currentTerm,rf.me)
					rf.state = LEADER
					rf.nextIndex = make([]int,len(rf.peers))
					rf.matchIndex = make([]int,len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
				//如果收到别的心跳则转换成follower
				//If AppendEntries RPC received from new leader: convert to follower
				case <- rf.heartBeat:
					rf.state = FOLLOWER
				//如果此轮选举超时则转换成follower
				//If election timeout elapses: start new election
				case <-time.After(time.Millisecond*time.Duration(rand.Int63n(400)+100)):
					rf.state = FOLLOWER
				}
			case LEADER:
				//定期发送心跳
				//Upon election: send initial empty AppendEntries RPCs
				//(heartbeat) to each server
				rf.sendAppendEntriesToAll(true)
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()
	return rf
}
