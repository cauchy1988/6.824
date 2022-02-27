package shardmaster


import "raft"
import "labrpc"
import "sync"
import (
	"labgob"
	"time"
	"sync/atomic"
	"sort"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead    int32 // set by Kill()

	rwLock sync.RWMutex
	configs []Config // indexed by config num
	clientInfoMap map[int32]ClientCachedInfo
	lastTerm int
}

type ClientCachedInfo struct {
	RequestId int32
	WrongLeader bool
	Err Err
	Config Config
}

type Op struct {
	// Your data here.
	OpType string
	Args interface{}
}

func (sm *ShardMaster) waitForInitCompleted() bool {
	term , leader := sm.rf.GetState()
	if !leader {
		return false
	}

	sm.rwLock.RLock()
	isInitCompleted := term <= sm.lastTerm
	sm.rwLock.RUnlock()

	if !isInitCompleted {
		_, currentTerm, leader := sm.rf.Start(Op{OpType: "NONE"})
		for leader && term == currentTerm && !isInitCompleted {
			time.Sleep(10 * time.Millisecond)

			sm.rwLock.RLock()
			isInitCompleted = term <= sm.lastTerm
			sm.rwLock.RUnlock()
			currentTerm, leader = sm.rf.GetState()
		}

		if !leader || currentTerm != term {
			return false
		}
	}

	return true
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.configChange("JOIN", args.ClientIdxInfo,  args, &reply.WrongLeader, &reply.Err)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sm.configChange("LEAVE", args.ClientIdxInfo, args, &reply.WrongLeader, &reply.Err)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.configChange("MOVE", args.ClientIdxInfo, args, &reply.WrongLeader, &reply.Err)
}

func (sm *ShardMaster) configChange(opType string, clientIdxInfo ClientIdxInfo,  args interface{}, wrongLeader *bool, err *Err) {
	*wrongLeader = false

	if !sm.waitForInitCompleted() {
		*wrongLeader = true
		return
	}

	{
		sm.rwLock.RLock()
		clientValue, ok := sm.clientInfoMap[clientIdxInfo.clientId]
		if ok && clientValue.RequestId == clientIdxInfo.requestId {
			*wrongLeader = clientValue.WrongLeader
			*err = clientValue.Err
			return
		}
		sm.rwLock.RUnlock()
	}

	tmpIndex, tmpTerm, tmpLeader := sm.rf.Start(Op{OpType : opType, Args : args})
	if !tmpLeader {
		*wrongLeader = true
		return
	}

	term, isLeader := sm.rf.GetState()
	for isLeader && !sm.killed() && term == tmpTerm && atomic.LoadInt32(&sm.rf.LastApplied) < int32(tmpIndex){
		term, isLeader = sm.rf.GetState()
	}

	if !isLeader || sm.killed() || term != tmpTerm {
		*wrongLeader = true
		return
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	reply.WrongLeader = false

	if !sm.waitForInitCompleted() {
		reply.WrongLeader = true
		return
	}

	{
		sm.rwLock.RLock()
		clientValue, ok := sm.clientInfoMap[args.ClientIdxInfo.clientId]
		if ok && clientValue.RequestId == args.ClientIdxInfo.requestId {
			reply.WrongLeader = clientValue.WrongLeader
			reply.Err = clientValue.Err
			reply.Config = clientValue.Config
			return
		}
		sm.rwLock.RUnlock()
	}

	tmpIndex, tmpTerm, tmpLeader := sm.rf.Start(Op{OpType : "QUERY", Args : args})
	if !tmpLeader {
		reply.WrongLeader = true
		return
	}

	term, isLeader := sm.rf.GetState()
	for isLeader && !sm.killed() && term == tmpTerm && atomic.LoadInt32(&sm.rf.LastApplied) < int32(tmpIndex){
		term, isLeader = sm.rf.GetState()
	}

	if !isLeader || sm.killed() || term != tmpTerm {
		reply.WrongLeader = true
		return
	}

	{
		sm.rwLock.RLock()
		reply.Err = sm.clientInfoMap[args.ClientIdxInfo.clientId].Err
		reply.WrongLeader = sm.clientInfoMap[args.ClientIdxInfo.clientId].WrongLeader
		reply.Config = sm.clientInfoMap[args.ClientIdxInfo.clientId].Config
		sm.rwLock.RUnlock()
	}
}

func (sm *ShardMaster) doBalance(config *Config, joinedGid []int, leftGid []int) {
	/*
	 * 遍历Shards，每一项检查是否在GIDS中: 不在的话放入remainGids中;
	 * 遍历的同时需要对每个Gid拥有的shards进行计数；
	 * 此外还需要得到总的Gid的个数;
	 * 算出每个Gid 应该得到的平均 shard的个数；
	 * 得到每个Gid的目标shard个数;
	 * 将gid按照shard个数的大小，按升序排序;
	 * 如果remainGid还有shards则将里面的填充最少的gid直至填充到目标大小;
	 * 如果remainGid没有shards, 则用目前最大的shard数的gid来填充最小的gid;
	 * 用上述方法不断进行"均匀", 过程中大家都往之前算好的目标shards数来做限制;
	*/

	gidNum := len(config.Groups)

	if 0 == gidNum {
		for i := 0; i < len(config.Shards); i++ {
			config.Shards[i] = 0
		}
		return
	}

	average := len(config.Shards) / gidNum
	aimedShardNum := make([]int, gidNum)
	for i:= 0; i < gidNum; i++ {
		aimedShardNum[i] = average
	}
	rem := len(config.Shards) % gidNum
	for i := gidNum - 1; i >= 0 && rem > 0; i-- {
		aimedShardNum[i]++
		rem--
	}

	gidNumMap := make(map[int][]int)
	gidArray := make([]int, 0)
	for k := range config.Groups {
		gidNumMap[k] = make([]int, 0)
		gidArray  = append(gidArray, k)
	}

	remainGids := make([]int, 0)

	for i := 0; i < len(config.Shards); i++ {
		currentGid := config.Shards[i]
		if _, ok := config.Groups[currentGid]; !ok {
			remainGids = append(remainGids, i)
		} else {
			gidNumMap[currentGid] = append(gidNumMap[currentGid], i)
		}
	}

	// do sort to gidArray
	// sort 'gidArray' in ascending order by 'len(gidNumMap[gidArray[i]]) < len(gidNumMap[gidArray[j]])'
	sort.Slice(gidArray, func(i, j int) bool {
		return len(gidNumMap[gidArray[i]]) < len(gidNumMap[gidArray[j]])
	})

	startIdx := 0
	for startIdx < gidNum && len(remainGids) > 0 {
		for len(gidNumMap[gidArray[startIdx]]) < aimedShardNum[startIdx] && len(remainGids) > 0 {
			config.Shards[remainGids[0]] = gidArray[startIdx]
			gidNumMap[gidArray[startIdx]] = append(gidNumMap[gidArray[startIdx]], remainGids[0])
			remainGids = remainGids[1:]
		}

		if len(gidNumMap[gidArray[startIdx]]) >= aimedShardNum[startIdx] {
			startIdx++
		}
	}

	endIdx := gidNum - 1
	for startIdx < endIdx {
		for len(gidNumMap[gidArray[startIdx]]) < aimedShardNum[startIdx] && len(gidNumMap[gidArray[endIdx]]) > aimedShardNum[endIdx] {
			transferringShard := gidNumMap[gidArray[endIdx]][0]
			gidNumMap[gidArray[endIdx]] = gidNumMap[gidArray[endIdx]][1:]
			gidNumMap[gidArray[startIdx]] = append(gidNumMap[gidArray[startIdx]], transferringShard)
			config.Shards[transferringShard] =  gidArray[startIdx]
		}

		if len(gidNumMap[gidArray[startIdx]]) >= aimedShardNum[startIdx] {
			startIdx++
		}

		if len(gidNumMap[gidArray[endIdx]]) <= aimedShardNum[endIdx] {
			endIdx--
		}
	}
}

func (sm *ShardMaster) deepCopyGidMap(config *Config) {
	newMap := make(map[int][]string)
	for k, v := range config.Groups {
		newMap[k] = v
	}
	config.Groups = newMap
}

func (sm *ShardMaster) applyLoop() {
	for !sm.killed() {
		appMsg := <-sm.applyCh

		sm.rwLock.Lock()

		index := appMsg.CommandIndex
		op := appMsg.Command.(Op)
		var clientIdxInfo *ClientIdxInfo = nil
		var realIdx = -1

		if "NONE" == op.OpType {
			// do nothing
		} else if "LEAVE" == op.OpType {
			args := op.Args.(LeaveArgs)
			clientIdxInfo = &args.ClientIdxInfo

			currentConfig := sm.configs[len(sm.configs) - 1]
			sm.deepCopyGidMap(&currentConfig)

			for i := 0; i < len(args.GIDs); i++ {
				delete(currentConfig.Groups, args.GIDs[i])
			}

			sm.doBalance(&currentConfig, nil, args.GIDs)

			currentConfig.Num = len(sm.configs)
			sm.configs = append(sm.configs, currentConfig)
		} else if "JOIN" == op.OpType {
			args := op.Args.(JoinArgs)
			clientIdxInfo = &args.ClientIdxInfo

			currentConfig := sm.configs[len(sm.configs) - 1]
			sm.deepCopyGidMap(&currentConfig)

			addedGid := make([]int, 0)
			for k, v  := range args.Servers {
				if _, ok := currentConfig.Groups[k]; !ok {
					addedGid = append(addedGid, k)
					currentConfig.Groups[k] = v
				}
			}

			sm.doBalance(&currentConfig, addedGid, nil)

			currentConfig.Num = len(sm.configs)
			sm.configs = append(sm.configs, currentConfig)
		} else if "MOVE" == op.OpType {
			args := op.Args.(MoveArgs)
			clientIdxInfo = &args.ClientIdxInfo

			currentConfig := sm.configs[len(sm.configs) - 1]
			sm.deepCopyGidMap(&currentConfig)
			if _, ok := currentConfig.Groups[args.GID]; ok && args.Shard >= 0 && args.Shard < len(currentConfig.Shards) {
				currentConfig.Shards[args.Shard] = args.GID
				currentConfig.Num = len(sm.configs)
				sm.configs = append(sm.configs, currentConfig)
			}
		} else if "QUERY" == op.OpType {
			args := op.Args.(QueryArgs)
			clientIdxInfo = &args.ClientIdxInfo
			realIdx = args.Num
		}

		if "NONE" != op.OpType {
			value, ok := sm.clientInfoMap[clientIdxInfo.clientId]
			if !ok {
				value = ClientCachedInfo{}
			}

			value.WrongLeader = false
			value.RequestId = clientIdxInfo.requestId
			value.Err = OK

			if "QUERY" == op.OpType {
				if realIdx < 0 || realIdx >= len(sm.configs) {
					realIdx = len(sm.configs) - 1
				}
				value.Config = sm.configs[realIdx]
			}

			sm.clientInfoMap[clientIdxInfo.clientId] = value
		}

		if !atomic.CompareAndSwapInt32(&sm.rf.LastApplied, int32(index) - 1, int32(index)) {
			panic("Fatal Error: lastApplied not apply in sequence!!!")
		}

		sm.rwLock.Unlock()
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.clientInfoMap = make(map[int32]ClientCachedInfo)
	go sm.applyLoop()

	return sm
}
