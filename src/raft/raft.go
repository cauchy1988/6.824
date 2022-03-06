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
	// "fmt"
	"labgob"
	"sync"
)
import "sync/atomic"
import "labrpc"

import "math/rand"
import (
	"time"
)


// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm int
	SnapshotState KvState
}

type LogEntry struct {
	Term int
	Command interface{}
}


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg

	// persistant states
	currentTerm int
	votedFor map[int] int
	log []LogEntry
	lastSnapshotIndex int
	lastSnapShortTerm int
	baseIdx int

	// volatile states for all servers
	commitIndex int
	LastApplied int32

	// volatile states for leader
	nextIndex map[int] int
	matchIndex map[int] int

	// leader information 
	leaderIndex int

	// election elapsed time
	electionElapsedTime int

	bCalled bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.leaderIndex == rf.me)
	rf.mu.Unlock()

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
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapShortTerm)
	e.Encode(rf.baseIdx)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) RaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.persister.RaftStateSize()
}


type KvState struct {
	LastIndex int
	LastTerm int
	InnerMap map[string]string
	ClientErrMap       map[int32]string
	ClientValueMap     map[int32]string
	ClientRequestIdMap map[int32]int32
}

func (rf *Raft) SaveSnapshot(kvState *KvState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.saveSnapshot(kvState)
}

func (rf *Raft) saveSnapshot(kvState *KvState) {
	x := new(bytes.Buffer)
	xx := labgob.NewEncoder(x)
	xx.Encode(kvState)
	xxx := x.Bytes()

	if kvState.LastIndex < rf.baseIdx + 1 {
		return
	}

	if rf.baseIdx + len(rf.log) >= kvState.LastIndex && rf.log[kvState.LastIndex - rf.baseIdx - 1].Term == kvState.LastTerm {
		rf.log = rf.log[kvState.LastIndex - rf.baseIdx:]
	} else {
		rf.log = []LogEntry{}
	}

	rf.baseIdx = kvState.LastIndex
	rf.lastSnapshotIndex = kvState.LastIndex
	rf.lastSnapShortTerm = kvState.LastTerm

	if rf.commitIndex < rf.baseIdx {
		rf.commitIndex = rf.baseIdx
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapShortTerm)
	e.Encode(rf.baseIdx)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, xxx)
}


func (rf *Raft) readSnapshot(data []byte) KvState{
	if data == nil || len(data) < 1 {
		panic("not any snapshot!!!")
	}

	var kvState KvState

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kvState) != nil {
		panic("decode snapshot error!!!")
	}

	return kvState
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int = 0
	var vote_info map[int] int = map[int] int{}
	var log []LogEntry = []LogEntry{}
	var lastSnapshotTerm = 0
	var lastSnapshotIndex = 0
	var base_idx int = 0
	if d.Decode(&term) != nil || d.Decode(&vote_info) != nil || d.Decode(&log) != nil || d.Decode(&lastSnapshotIndex) != nil || d.Decode(&lastSnapshotTerm) != nil  || d.Decode(&base_idx) != nil{
		// fmt.Println("readPersist error.")
	} else {
		rf.currentTerm = term
		rf.votedFor = vote_info
		rf.log = log
		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapShortTerm = lastSnapshotTerm
		rf.baseIdx = base_idx
	}
}

func(rf *Raft) InitCompleted() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastAppliedLoad := int(atomic.LoadInt32(&rf.LastApplied))
	isInitCompleted :=  lastAppliedLoad  > 0 && rf.log[lastAppliedLoad - 1 - rf.baseIdx].Term >= rf.currentTerm
	return isInitCompleted
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	Data []byte
}

type InstallSnapshotReply struct {
	Term int
	Executed bool
	ExecutedIndex int
}

func(rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Executed = false

	if rf.currentTerm > args.Term {
		return
	}

	if rf.currentTerm != args.Term {
		rf.currentTerm = args.Term
	}

	if rf.leaderIndex != args.LeaderId {
		rf.leaderIndex = args.LeaderId
	}

	if nil == args.Data {
		panic("nil == args.Data")
		return
	}

	rf.bCalled = true

	kvState := rf.readSnapshot(args.Data)
	currentLastApplied := int(atomic.LoadInt32(&rf.LastApplied))
	if currentLastApplied >= kvState.LastIndex {
		reply.Executed = true
		reply.ExecutedIndex = kvState.LastIndex
		return
	}

	if rf.currentTerm !=  args.Term {
		rf.currentTerm = args.Term
	}

	if rf.leaderIndex != args.LeaderId {
		rf.leaderIndex = args.LeaderId
	}

	reply.Executed = true
	reply.ExecutedIndex = kvState.LastIndex

	rf.saveSnapshot(&kvState)

	applyMsg := ApplyMsg{
		CommandValid: false,
		SnapshotState: kvState,
	}
	rf.applyCh <- applyMsg
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Println("RequestVote me:", rf.me, ", args:", *args, ", leaderIndex:", rf.leaderIndex, ", term:", rf.currentTerm)

	changed := false
	defer func() {
		if changed {
			rf.persist()
		}
	}()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else {
		if (args.Term > rf.currentTerm && rf.leaderIndex != -1) {
			rf.leaderIndex = -1
		}

		if args.Term != rf.currentTerm {
			rf.currentTerm = args.Term
			changed = true
		}

		log_len := len(rf.log) + rf.baseIdx
		if _, ok := rf.votedFor[args.Term];
			(!ok || rf.votedFor[args.Term] == args.CandidateId) &&
				(log_len == rf.baseIdx && (rf.baseIdx == 0 || args.LastLogTerm > rf.lastSnapShortTerm || args.LastLogTerm == rf.lastSnapShortTerm && args.LastLogIndex >= log_len) || (log_len > rf.baseIdx && ((args.LastLogTerm > rf.log[log_len - 1 - rf.baseIdx].Term) ||
					(args.LastLogTerm == rf.log[log_len - 1 - rf.baseIdx].Term &&
						args.LastLogIndex >= log_len)))) {

			rf.votedFor[args.Term] = args.CandidateId
			if !ok {
				changed = true
			}
			rf.bCalled = true
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	}
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Println("AppendEntries me:", rf.me, ", args:", *args, ", leaderIndex:", rf.leaderIndex, ", term:", rf.currentTerm, ", commitIndex:", rf.commitIndex)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	changed := false
	defer func() {
		if changed {
			rf.persist()
		}
	}()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		if rf.leaderIndex != args.LeaderId {
			rf.leaderIndex = args.LeaderId
		}

		if rf.currentTerm != args.Term {
			rf.currentTerm = args.Term
			changed = true
		}

		rf.bCalled = true

		reply.Success =true

		// other logic remain for 2B & 2C
		if args.PrevLogIndex > len(rf.log) + rf.baseIdx {
			reply.Success = false
			return
		}

		if args.PrevLogIndex > rf.baseIdx && rf.log[args.PrevLogIndex - 1 - rf.baseIdx].Term != args.PrevLogTerm {
			// delete all log [args.PrevLogIndex - 1, ++)
			rf.log = rf.log[:args.PrevLogIndex - 1 - rf.baseIdx]
			changed = true
			reply.Success = false
			return
		}

		for i := 0; i < len(args.Entries); i++ {
			if args.PrevLogIndex + i >= len(rf.log) + rf.baseIdx {
				// local_idx := len(rf.log)

				rf.log = append(rf.log, args.Entries[i:]...)
				changed = true

				/*
				for j := i; j < len(args.Entries); j++ {
					fmt.Println("copied me:", rf.me, ", command:", args.Entries[j].Command, ", commandindex:", local_idx + j - i + 1)
				}
				*/

				break
			} else {
				if 	args.PrevLogIndex + i - rf.baseIdx < 0 {
					continue
				}

				if rf.log[args.PrevLogIndex + i - rf.baseIdx].Term != args.Entries[i].Term {
					rf.log = rf.log[:args.PrevLogIndex + i - rf.baseIdx]
					changed = true
					i--
				}
			}
		}

		if rf.commitIndex < rf.lastSnapshotIndex {
			snapshotBytes := rf.persister.ReadSnapshot()
			if nil != snapshotBytes {
				kvState := rf.readSnapshot(snapshotBytes)
				rf.applyCh <- ApplyMsg{CommandValid:false, SnapshotState: kvState}
			}
			rf.commitIndex = rf.lastSnapshotIndex
		}

		if args.LeaderCommit > rf.commitIndex {
			orig_commit_idx := rf.commitIndex
			if args.LeaderCommit < len(rf.log) + rf.baseIdx {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.log) + rf.baseIdx
			}

			apply_msg := ApplyMsg{
				CommandValid: true,
			}
			for j := orig_commit_idx + 1; j <= rf.commitIndex; j++ {
				apply_msg.Command = rf.log[j - 1 - rf.baseIdx].Command
				// fmt.Println("me:", rf.me, ", command:", apply_msg.Command, ", commandindex:", apply_msg.CommandIndex)
				apply_msg.CommandIndex = j
				apply_msg.CommandTerm = rf.log[j - 1 - rf.baseIdx].Term
				rf.applyCh <- apply_msg
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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


func(rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	isLeader = (rf.leaderIndex == rf.me)
	term = rf.currentTerm
	if isLeader {
		rf.log = append(rf.log, LogEntry{Command: command, Term: term})
		rf.persist()
		index = len(rf.log) + rf.baseIdx
		rf.matchIndex[rf.me] = index
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft)commitLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.leaderIndex == rf.me {
			if rf.commitIndex < int(atomic.LoadInt32(&rf.LastApplied)) {
				rf.commitIndex = int(atomic.LoadInt32(&rf.LastApplied))
			}

			if rf.commitIndex < rf.lastSnapshotIndex {
				snapshotBytes := rf.persister.ReadSnapshot()
				if nil != snapshotBytes {
					kvState := rf.readSnapshot(snapshotBytes)
					rf.applyCh <- ApplyMsg{CommandValid:false, SnapshotState: kvState}
				}
				rf.commitIndex = rf.lastSnapshotIndex
			}

			try_idx := rf.commitIndex + 1
			for {
				if try_idx > len(rf.log) + rf.baseIdx || rf.log[try_idx - 1 - rf.baseIdx].Term == rf.currentTerm {
					break
				}
				try_idx++
			}
			if try_idx <= len(rf.log) + rf.baseIdx {
				// fmt.Println("leader:", rf.me, ", start commit loop, try_idx:", try_idx, "， commit idx:", rf.commitIndex)

				count := 0
				for j := 0; j < len(rf.peers); j++ {
					if (rf.matchIndex[j] >= try_idx) {
						count++;
					}
				}

				if count > (len(rf.peers) / 2) {
					apply_msg := ApplyMsg{
						CommandValid: true,
					}
					for i := rf.commitIndex + 1; i <= try_idx; i++ {
						apply_msg.Command = rf.log[i - 1 - rf.baseIdx].Command
						apply_msg.CommandIndex = i
						apply_msg.CommandTerm = rf.log[i - 1 - rf.baseIdx].Term
						// fmt.Println(" before send channel leader:", rf.me, ", CommandIndex:", apply_msg.CommandIndex)
						rf.applyCh <- apply_msg
						// fmt.Println(" after send channel leader:", rf.me, ", CommandIndex:", apply_msg.CommandIndex)
					}
					rf.commitIndex = try_idx
				}

				// fmt.Println("leader:", rf.me, ", end commit loop.")
			}
		}
		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft)runLoop() {
	for !rf.killed() {
		// fmt.Println("runLoop-Append start:", rf.me)
		if tmp_term, is_leader := rf.GetState(); is_leader {
			// fmt.Println("runLoop-Append me leader:", rf.me)
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go func(idx int) {
						rf.mu.Lock()
						if rf.currentTerm != tmp_term {
							rf.mu.Unlock()
							return
						}

						// 某些落后太多的副本需要直接传snapshot过去
						prevIdx := rf.nextIndex[idx] - 1
						if prevIdx == 0 && rf.baseIdx > 0 || prevIdx > 0 && prevIdx < rf.baseIdx {
							installSnapshotargs := &InstallSnapshotArgs{
								Term: rf.currentTerm,
								LeaderId: rf.me,
								Data: rf.persister.ReadSnapshot(),
							}
							rf.mu.Unlock()


							installSnapshotReply := &InstallSnapshotReply{}
							ret := rf.sendInstallSnapshot(idx, installSnapshotargs, installSnapshotReply)

							if !ret {
								installSnapshotReply.Executed = false
								installSnapshotReply.Term = 0
							}

							//fmt.Println("send snapshot, me:", rf.me, ", to:", idx, ", ret:", ret, ", executed:", installSnapshotReply.Executed, ", term:", installSnapshotReply.Term, ", executed idx:", installSnapshotReply.ExecutedIndex, ", baseIdx:", rf.baseIdx)

							rf.mu.Lock()
							if installSnapshotReply.Term > rf.currentTerm {
								rf.currentTerm = installSnapshotReply.Term
								rf.leaderIndex = -1
								rf.persist()
							}

							if installSnapshotReply.Executed {
								rf.nextIndex[idx] = installSnapshotReply.ExecutedIndex + 1
							}

							rf.mu.Unlock()
							return
						}

						append_args := &AppendEntriesArgs{
							Term : tmp_term,
							LeaderId : rf.me,
							PrevLogIndex : prevIdx,
							PrevLogTerm : 0,
							Entries : []LogEntry{},
							LeaderCommit : rf.commitIndex,
						}
						if append_args.PrevLogIndex - rf.baseIdx > 0 {
							append_args.PrevLogTerm = rf.log[append_args.PrevLogIndex - 1 - rf.baseIdx].Term
						} else if rf.baseIdx > 0{
							append_args.PrevLogTerm = rf.lastSnapShortTerm
						}
						append_args.Entries = rf.log[append_args.PrevLogIndex - rf.baseIdx:]

						rf.mu.Unlock()

						// fmt.Println("runLoop-Append append_args:", *append_args, ", idx:", idx, ", leaderIndex:", rf.leaderIndex, ", term:", rf.currentTerm)

						append_reply := &AppendEntriesReply{}
						ret := rf.sendAppendEntries(idx, append_args, append_reply)

						// fmt.Println("runLoop-Append append_args:", *append_args, ", idx:", idx, ", leaderIndex:", rf.leaderIndex, ", term:", rf.currentTerm, ", reply:", append_reply)

						if !ret {
							append_reply.Success = false
							append_reply.Term = 0
						}

						rf.mu.Lock()
						if ret && append_reply.Term > rf.currentTerm{
							rf.currentTerm = append_reply.Term
							rf.leaderIndex = -1
							rf.persist()
						}

						if ret && rf.currentTerm == tmp_term {
							if append_reply.Success {
								if append_args.PrevLogIndex + 1 + len(append_args.Entries) > rf.nextIndex[idx] {
									rf.nextIndex[idx] = append_args.PrevLogIndex + 1 + len(append_args.Entries)
								}

								if rf.matchIndex[idx] < rf.nextIndex[idx] - 1 {
									rf.matchIndex[idx] = rf.nextIndex[idx] - 1
								}
							} else {
								if rf.nextIndex[idx] > 100 {
									rf.nextIndex[idx] -= 100
								} else if rf.nextIndex[idx] > 50 {
									rf.nextIndex[idx] -= 50
								} else if rf.nextIndex[idx] > 10 {
									rf.nextIndex[idx] -= 10
								} else if rf.nextIndex[idx] > 1{
									rf.nextIndex[idx] -= 1
								}
							}
						}
						rf.mu.Unlock()
					}(i)
				}
			}

			// fmt.Println("runLoop-Append me leader end1:", rf.me)
			time.Sleep(50 * time.Millisecond)
			// fmt.Println("runLoop-Append me leader end2:", rf.me)
		} else {
			// fmt.Println("runLoop-Append me:", rf.me)
			time.Sleep(time.Duration(rf.electionElapsedTime) * time.Millisecond)

			rf.mu.Lock()
			// fmt.Println("me:", rf.me, " awake from sleep1.")

			if rf.leaderIndex == rf.me {
				rf.mu.Unlock()
				continue
			}

			// fmt.Println("me:", rf.me, " awake from sleep2.")

			be_called := rf.bCalled
			if be_called {
				rf.bCalled = false
				rf.mu.Unlock()
				continue
			}

			//  fmt.Println("me:", rf.me, " awake from sleep3.")

			rf.leaderIndex = -1
			rf.currentTerm++
			rf.persist()
			rf.votedFor[rf.currentTerm] = rf.me

			candidate_id := rf.me
			candidate_term := rf.currentTerm
			log_idx := len(rf.log) + rf.baseIdx
			log_term := 0
			if log_idx - rf.baseIdx > 0 {
				log_term = rf.log[log_idx - 1 - rf.baseIdx].Term
			} else if rf.baseIdx > 0 {
				log_term = rf.lastSnapShortTerm
			}

			rf.mu.Unlock()

			if !be_called {
				// send RequestVote Rpc
				req_args := &RequestVoteArgs{
					Term : candidate_term,
					CandidateId : candidate_id,
					LastLogIndex : log_idx,
					LastLogTerm : log_term,
				}

				go func() {
					reply_chan := make(chan RequestVoteReply, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						if i != rf.me {
							go func(idx int) {
								// fmt.Println("runLoop vote req_args:", *req_args, ", idx:", idx, ", leaderIndex:", rf.leaderIndex, ", term:", rf.currentTerm)

								req_reply := &RequestVoteReply{}
								ret := rf.sendRequestVote(idx, req_args, req_reply)
								if !ret {
									req_reply.VoteGranted = false
									req_reply.Term = 0
								}
								reply_chan <- (*req_reply)
							}(i)
						}
					}

					var vote_granted_num int32 = 1
					for i := 0; i < len(rf.peers); i++ {
						one_reply := <-reply_chan
						if one_reply.VoteGranted {
							atomic.AddInt32(&vote_granted_num, 1)
							if atomic.LoadInt32(&vote_granted_num) > int32(len(rf.peers) / 2) {
								rf.mu.Lock()
								if candidate_term == rf.currentTerm  && rf.leaderIndex != rf.me{
									// fmt.Println("me:", rf.me, ", to be a leader.")
									rf.nextIndex = map[int] int{}
									rf.matchIndex = map[int] int{}
									for j := 0; j < len(rf.peers); j++ {
										rf.nextIndex[j] = len(rf.log) + 1 + rf.baseIdx
										if j != rf.me {
											rf.matchIndex[j] = 0
										} else {
											rf.matchIndex[j] = len(rf.log) + rf.baseIdx
										}
									}
									rf.leaderIndex = rf.me
									// fmt.Println("me:", rf.me, ", to be leader.")
								}
								rf.mu.Unlock()
							}
						} else {
							rf.mu.Lock()
							if one_reply.Term > rf.currentTerm {
								// fmt.Println("me:", rf.me, ", to be reset.")

								rf.currentTerm = one_reply.Term
								rf.leaderIndex = -1
								rf.persist()
							}
							rf.mu.Unlock()
						}
					}
				}()
			}
		}
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
	rf.applyCh = applyCh

	// persistent states for all servers
	rf.currentTerm = 0
	rf.votedFor = make(map[int] int)
	rf.log = []LogEntry{}
	rf.lastSnapshotIndex = 0
	rf.lastSnapShortTerm = 0
	rf.baseIdx = 0

	// volatile states for all servers
	rf.commitIndex = 0
	rf.LastApplied = 0

	rf.leaderIndex = -1;

	rr := rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.electionElapsedTime = int(rr.Int31() % 200) + 230

	rf.bCalled = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.runLoop()
	go rf.commitLoop()

	return rf
}
