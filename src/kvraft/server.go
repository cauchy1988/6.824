package kvraft

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"sync/atomic"
	"time"
	// "fmt"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType     string
	Key        string
	Value      string
	ClientIdx  int32
	RequestIdx int32
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	innerMap map[string]string

	rwLock sync.RWMutex
	clientErrMap       map[int32]Err
	clientValueMap     map[int32]string
	clientRequestIdMap map[int32]int32
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// fmt.Println("Get Handler: args-", args)

	tmp_term , leader := kv.rf.GetState()
	if !leader {
		reply.Err = ErrWrongLeader
		return
	}

	if !kv.rf.InitCompleted() {
		_, current_term, _ := kv.rf.Start(Op{OpType: "None"})
		tmp_term, leader = kv.rf.GetState()
		for leader && tmp_term == current_term && !kv.rf.InitCompleted() {
			time.Sleep(10 * time.Millisecond)
			tmp_term, leader = kv.rf.GetState()
		}

		if !leader || current_term != tmp_term {
			reply.Err = ErrWrongLeader
			return
		}
	}

	{
		kv.rwLock.RLock()
		_, ok := kv.clientErrMap[args.ClientId]
		_, ok1 :=kv.clientRequestIdMap[args.ClientId]
		if ok  && ok1 && kv.clientRequestIdMap[args.ClientId] == args.RequestId {
			reply.Err = kv.clientErrMap[clientIdx]
			if reply.Err == OK {
				reply.Value = kv.clientValueMap[args.ClientId]
			}
			kv.rwLock.RUnlock()
			return
		}
		kv.rwLock.RUnlock()
	}

	tmpIndex, tmpTerm, tmpLeader := kv.rf.Start(Op{Key: args.Key, OpType: "Get", ClientIdx: args.ClientId, RequestIdx: args.RequestId})
	if !tmpLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//  fmt.Println("Get Handler: tmpIndex-", tmpIndex, ", tmpLeader:", tmpLeader,  "lastApplied:", kv.rf.LastApplied)

	term, isLeader := kv.rf.GetState()
	for !kv.killed() && isLeader && term == tmpTerm && atomic.LoadInt32(&kv.rf.LastApplied) < int32(tmpIndex){
		// time.Sleep(time.Duration(10) * time.Millisecond)
		term, isLeader = kv.rf.GetState()
		//  fmt.Println("Get Handler loop: tmpIndex-", tmpIndex, ", tmpLeader:", tmpLeader,  "lastApplied:", kv.rf.LastApplied)
	}

	if !isLeader || kv.killed() || term != tmpTerm {
		reply.Err = ErrWrongLeader
		return
	}

	{
		kv.rwLock.RLock()
		reply.Err = kv.clientErrMap[args.ClientId]
		reply.Value = kv.clientValueMap[args.ClientId]
		// fmt.Println("args-requestid:", args.RequestId, ", realRequestId:", kv.clientRequestIdMap[args.ClientId])
		kv.rwLock.RUnlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// fmt.Println("Put Handler: args-", args)
	tmp_term , leader := kv.rf.GetState()
	if !leader {
		reply.Err = ErrWrongLeader
		return
	}

	if !kv.rf.InitCompleted() {
		_, current_term, _ := kv.rf.Start(Op{OpType: "None"})
		tmp_term, leader = kv.rf.GetState()
		for leader && current_term == tmp_term && !kv.rf.InitCompleted() {
			time.Sleep(10 * time.Millisecond)
			tmp_term, leader = kv.rf.GetState()
		}

		if !leader || current_term != tmp_term {
			reply.Err = ErrWrongLeader
			return
		}
	}


	{
		kv.rwLock.RLock()
		_, ok := kv.clientErrMap[args.ClientId]
		_, ok1 :=kv.clientRequestIdMap[args.ClientId]
		if ok  && ok1 && kv.clientRequestIdMap[args.ClientId] == args.RequestId {
			reply.Err = kv.clientErrMap[clientIdx]
			kv.rwLock.RUnlock()
			return
		}
		kv.rwLock.RUnlock()
	}

	tmpIndex, tmpTerm, tmpLeader := kv.rf.Start(Op{Key: args.Key, Value: args.Value, OpType: args.Op, ClientIdx: args.ClientId, RequestIdx: args.RequestId})
	if !tmpLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// fmt.Println("Put Handler: tmpIndex-", tmpIndex, ", tmpLeader:", tmpLeader,  "lastApplied:", kv.rf.LastApplied)

	term, isLeader := kv.rf.GetState()
	for !kv.killed() && isLeader && term == tmpTerm && atomic.LoadInt32(&kv.rf.LastApplied) < int32(tmpIndex){
		// time.Sleep(time.Duration(10) * time.Millisecond)
		term, isLeader = kv.rf.GetState()
		// fmt.Println("Put Handler loop: tmpIndex-", tmpIndex, ", tmpLeader:", tmpLeader,  "lastApplied:", kv.rf.LastApplied)
	}

	if !isLeader || kv.killed() || term != tmpTerm {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
}

func (kv *KVServer) ApplyLoop() {
	for !kv.killed() {
		appMsg := <-kv.applyCh
		index := appMsg.CommandIndex

		op := appMsg.Command.(Op)
		if "Put" == op.OpType {
			kv.innerMap[op.Key] = op.Value
		} else if "Append" == op.OpType {
			if value, ok := kv.innerMap[op.Key]; ok {
				kv.innerMap[op.Key] = value + op.Value
			} else {
				kv.innerMap[op.Key] = op.Value
			}
		}

		if "None" != op.OpType {
			kv.rwLock.Lock()
			kv.clientErrMap[op.ClientIdx] = OK
			kv.clientRequestIdMap[op.ClientIdx] = op.RequestIdx
			if "Get" == op.OpType {
				if value, ok := kv.innerMap[op.Key]; ok {
					kv.clientValueMap[op.ClientIdx] = value
				} else {
					kv.clientErrMap[op.ClientIdx] = ErrNoKey
					kv.clientValueMap[op.ClientIdx] = ""
				}
			}
			kv.rwLock.Unlock()
		}

		if !atomic.CompareAndSwapInt32(&kv.rf.LastApplied, int32(index) - 1, int32(index)) {
			panic("Fatal Error: lastApplied not apply in sequence!!!")
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.innerMap = make(map[string]string)
	kv.clientValueMap = make(map[int32]string)
	kv.clientErrMap = make(map[int32]Err)
	kv.clientRequestIdMap = make(map[int32]int32)

	go kv.ApplyLoop()

	return kv
}
