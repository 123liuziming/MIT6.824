package shardmaster

import (
	"6.824/raft"
	"container/heap"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const (
	METHODJOIN  = 0
	METHODLEAVE = 1
)

type ReBalanceEntry struct {
	count  int
	gid    int
	shards []int
}

type RbQue []ReBalanceEntry

func (r *RbQue) Len() int {
	return len(*r)
}

// 小顶
func (r *RbQue) Less(i, j int) bool {
	return (*r)[i].count < (*r)[j].count
}

func (r *RbQue) Swap(i, j int) {
	(*r)[i], (*r)[j] = (*r)[j], (*r)[i]
}

func (r *RbQue) Push(x interface{}) {
	*r = append(*r, x.(ReBalanceEntry))
}

func (r *RbQue) Pop() interface{} {
	n := len(*r)
	x := (*r)[n-1]
	*r = (*r)[:n-1]
	return x
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	ack    map[int64]int64
	execCh map[int]chan Op
	killCh chan bool

	configs []Config // indexed by config num
}

type Op struct {
	ClientId  int64
	RequestId int64
	// Move, Join, Leave, Query
	Command string
	// 一些参数
	// Join
	JoinServers map[int][]string // new GID -> servers mappings
	// Leave
	LeaveGIDs []int
	// Move
	MoveShard int
	MoveGID   int
	// Query
	QueryNum    int
	QueryConfig Config
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	operation := Op{JoinServers: args.Servers, ClientId: args.ClientId, RequestId: args.RequestId, Command: "Join"}
	wrongLeader := sm.handleRequest(&operation)
	reply.WrongLeader = wrongLeader
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	operation := Op{LeaveGIDs: args.GIDs, ClientId: args.ClientId, RequestId: args.RequestId, Command: "Leave"}
	wrongLeader := sm.handleRequest(&operation)
	reply.WrongLeader = wrongLeader
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	operation := Op{MoveShard: args.Shard, MoveGID: args.GID, ClientId: args.ClientId, RequestId: args.RequestId, Command: "Move"}
	wrongLeader := sm.handleRequest(&operation)
	reply.WrongLeader = wrongLeader
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	operation := Op{QueryNum: args.Num, ClientId: args.ClientId, RequestId: args.RequestId, Command: "Query"}
	wrongLeader := sm.handleRequest(&operation)
	reply.WrongLeader, reply.Config = wrongLeader, operation.QueryConfig
}

func (sm *ShardMaster) sameOperation(op1 *Op, op2 *Op) bool {
	return op1.RequestId == op2.RequestId && op1.ClientId == op2.ClientId && op1.Command == op2.Command
}

func (sm *ShardMaster) handleRequest(op *Op) bool {
	index, _, isLeader := sm.rf.Start(*op)
	if !isLeader {
		return true
	}
	_, ok := sm.execCh[index]
	if !ok {
		sm.execCh[index] = make(chan Op, 1)
	}
	select {
	case operation := <-sm.execCh[index]:
		op.QueryConfig = operation.QueryConfig
		return !sm.sameOperation(op, &operation)
	case <-time.After(time.Duration(240) * time.Millisecond):
		return true
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.killCh <- true
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) applyOperation(op *Op) *Config {
	switch op.Command {
	case "Join":
		sm.applyJoin(op)
	case "Leave":
		sm.applyLeave(op)
	case "Move":
		sm.applyMove(op)
	case "Query":
		index := op.QueryNum
		if index < 0 || index >= len(sm.configs) {
			index = len(sm.configs) - 1
		}
		return &sm.configs[index]
	}
	return nil
}

func (sm *ShardMaster) applyMove(op *Op) {
	newConfig := sm.getCurrentConfigCopy()
	if _, ok := newConfig.Groups[op.MoveGID]; ok {
		newConfig.Shards[op.MoveShard] = op.MoveGID
	} else {
		return
	}
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) applyLeave(op *Op) {
	newConfig := sm.getCurrentConfigCopy()
	for _, gid := range op.LeaveGIDs {
		delete(newConfig.Groups, gid)
		sm.reBalanceLeave(&newConfig, gid)
	}
	sm.configs = append(sm.configs, newConfig)
	//log.Printf("leave %v! shard is %v\n", op.LeaveGIDs, newConfig.Shards)
}

func (sm *ShardMaster) applyJoin(op *Op) {
	newConfig := sm.getCurrentConfigCopy()
	for k, v := range op.JoinServers {
		vCopy := make([]string, len(v))
		copy(vCopy, v)
		newConfig.Groups[k] = vCopy
		sm.reBalanceJoin(&newConfig, k)
	}
	sm.configs = append(sm.configs, newConfig)
	//log.Printf("join! shard is %v\n", newConfig.Shards)
}

// method
func (sm *ShardMaster) reBalanceJoin(config *Config, gid int) {
	pq := sm.computePriorityQue(config, METHODJOIN, -1)
	if len(pq) == 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = gid
		}
		return
	}
	minCount := NShards / (len(pq) + 1)
	for i := 0; i < minCount; i++ {
		front := heap.Pop(&pq).(ReBalanceEntry)
		config.Shards[front.shards[0]] = gid
		front.shards, front.count = front.shards[1:], front.count+1
		heap.Push(&pq, front)
	}
}

func (sm *ShardMaster) reBalanceLeave(config *Config, gid int) {
	pq := sm.computePriorityQue(config, METHODLEAVE, gid)
	//if pq.Len() == 0 {
	countMap := sm.createCountMap(config, METHODLEAVE, gid)
	for gid, _ := range config.Groups {
		flag := false
		for g, _ := range countMap {
			if gid == g {
				flag = true
				break
			}
		}
		if !flag {
			heap.Push(&pq, ReBalanceEntry{
				count:  0,
				gid:    gid,
				shards: []int{},
			})
		}
	}
	if pq.Len() == 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
		return
	}
	for s, g := range config.Shards {
		if g == gid {
			front := heap.Pop(&pq).(ReBalanceEntry)
			config.Shards[s] = front.gid
			front.count = front.count + 1
			front.shards = append(front.shards, s)
			heap.Push(&pq, front)
		}
	}

}

func (sm *ShardMaster) createCountMap(config *Config, method int, groupId int) map[int][]int {
	countMap := make(map[int][]int)
	for shard, gid := range config.Shards {
		if gid == 0 || (method == METHODLEAVE && groupId == gid) {
			continue
		}
		_, ok := countMap[gid]
		if !ok {
			countMap[gid] = make([]int, 1)
			countMap[gid][0] = shard
		} else {
			countMap[gid] = append(countMap[gid], shard)
		}
	}
	return countMap
}

func (sm *ShardMaster) computePriorityQue(config *Config, method int, groupId int) RbQue {
	countMap := sm.createCountMap(config, method, groupId)
	que := RbQue{}
	heap.Init(&que)
	for gid, v := range countMap {
		if method == METHODJOIN {
			heap.Push(&que, ReBalanceEntry{count: -len(v), shards: v, gid: gid})
		} else if method == METHODLEAVE {
			heap.Push(&que, ReBalanceEntry{count: len(v), shards: v, gid: gid})
		}
	}
	return que
}

func (sm *ShardMaster) getCurrentConfig() Config {
	if len(sm.configs) == 0 {
		return Config{}
	} else {
		return sm.configs[len(sm.configs)-1]
	}
}

func (sm *ShardMaster) getCurrentConfigCopy() Config {
	if len(sm.configs) == 0 {
		return Config{}
	} else {
		now := sm.getCurrentConfig()
		return sm.deepCopyConfig(now)
	}
}

func (sm *ShardMaster) deepCopyConfig(config Config) Config {
	newConfig := Config{Num: config.Num + 1, Shards: [10]int{}, Groups: map[int][]string{}}
	for i, val := range config.Shards {
		newConfig.Shards[i] = val
	}
	for k, v := range config.Groups {
		newConfig.Groups[k] = v
	}
	return newConfig
}

func (sm *ShardMaster) isDuplicated(req, clientId int64) bool {
	if prevReq, ok := sm.ack[clientId]; ok {
		return req <= prevReq
	}
	return false
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
	sm.applyCh = make(chan raft.ApplyMsg, 100)
	sm.killCh = make(chan bool, 1)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.execCh = make(map[int]chan Op)
	sm.ack = make(map[int64]int64)

	// Your code here.
	// 有一个线程从applyCh中读出已经大于半数的log条目
	// 读出后执行操作，执行操作后将结果放到execResultCh中
	go func() {
		for {
			sm.mu.Lock()
			select {
			case applyMsg := <-sm.applyCh:
				var op Op
				var ok bool
				if op, ok = applyMsg.Command.(Op); !ok {
					break
				}
				if !sm.isDuplicated(op.RequestId, op.ClientId) {
					cfg := sm.applyOperation(&op)
					if cfg != nil {
						op.QueryConfig = *cfg
					}
					sm.ack[op.ClientId] = op.RequestId
				}
				// 重置channel
				_, ok = sm.execCh[applyMsg.CommandIndex]
				if !ok {
					sm.execCh[applyMsg.CommandIndex] = make(chan Op, 1)
				}
				select {
				case <-sm.execCh[applyMsg.CommandIndex]:
				default:
				}
				sm.execCh[applyMsg.CommandIndex] <- op
			case <-sm.killCh:
				return
			}
			sm.mu.Unlock()
		}
	}()

	return sm
}
