package kvraft

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync/atomic"
)


var clientIdx int32 = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int32
	requestId int32
	leaderIdx int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = atomic.AddInt32(&clientIdx, 1)
	ck.requestId = 1
	ck.leaderIdx = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	realLeader := ck.leaderIdx
	getArgs := GetArgs{Key: key, ClientId: ck.clientId, RequestId: atomic.AddInt32(&ck.requestId, 1)}
	getReply := GetReply{Err: ErrWrongLeader}
	ck.servers[realLeader].Call("KVServer.Get", &getArgs, &getReply)
	for ErrWrongLeader == getReply.Err {
		realLeader = (realLeader + 1) % (int64)(len(ck.servers))
		getReply = GetReply{Err: ErrWrongLeader}
		ck.servers[realLeader].Call("KVServer.Get", &getArgs, &getReply)
	}

	ck.leaderIdx = realLeader

	if ErrNoKey == getReply.Err {
		return ""
	}

	return getReply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	realLeader := ck.leaderIdx
	putAppendArgs := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, RequestId: atomic.AddInt32(&ck.requestId, 1)}
	putAppendReply := PutAppendReply{Err: ErrWrongLeader}
	ck.servers[realLeader].Call("KVServer.PutAppend", &putAppendArgs, &putAppendReply)
	for ErrWrongLeader == putAppendReply.Err {
		realLeader = (realLeader + 1) % (int64)(len(ck.servers))
		putAppendReply = PutAppendReply{Err: ErrWrongLeader}
		ck.servers[realLeader].Call("KVServer.PutAppend", &putAppendArgs, &putAppendReply)
	}

	ck.leaderIdx = realLeader
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
