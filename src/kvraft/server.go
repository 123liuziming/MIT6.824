package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1

const RequestTimeOut = time.Duration(240) * time.Millisecond

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
	Command   string
	RequestId uint64
	ClientId  int64
	Key       string
	Value     string
}

type OpResult struct {
	Operation Op
	Error     Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// data映射
	data map[string]string
	// ack,对应每个client的最大的已处理的id
	ack map[int64]uint64
	// 是否被kill
	isKilled int32
	// request map
	requestMap map[int]chan OpResult
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	operation := Op{Command: "Get", RequestId: args.Base.RequestId, ClientId: args.Base.ClientId, Key: args.Key}
	result := kv.startCommand(operation)
	if result.Error != OK {
		reply.Err = result.Error
		return
	}
	reply.Err, reply.Value = result.Error, result.Operation.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	operation := Op{Command: args.Op, RequestId: args.Base.RequestId, ClientId: args.Base.ClientId, Key: args.Key, Value: args.Value}
	result := kv.startCommand(operation)
	reply.Err = result.Error
}

func (kv *KVServer) startCommand(op Op) OpResult {
	index, _, isLeader := kv.rf.Start(op)
	//DPrintf("index, isLeader are: %d %t", index, isLeader)
	if !isLeader {
		return OpResult{Error: ErrWrongLeader}
	}

	kv.mu.Lock()
	if _, ok := kv.requestMap[index]; !ok {
		kv.requestMap[index] = make(chan OpResult, 1)
	}
	kv.mu.Unlock()

	select {
	case result := <-kv.requestMap[index]:
		{
			//DPrintf("index %d, there is a response", index)
			if result.Operation.ClientId == op.ClientId && result.Operation.RequestId == op.RequestId {
				return result
			}
			return OpResult{Error: "ErrUnMatch"}
		}
	case <-time.After(RequestTimeOut):
		{
			//DPrintf("timeout request")
			// 超时了，返回
			return OpResult{Error: "ErrTimeOut"}
		}
	}

}

func (kv *KVServer) isDuplicated(op Op) bool {
	reqId, ok := kv.ack[op.ClientId]
	return ok && reqId >= op.RequestId
}

func (kv *KVServer) applyOperation(op Op) OpResult {
	result := OpResult{Operation: op, Error: OK}
	switch op.Command {
	case "Get":
		{
			if val, ok := kv.data[op.Key]; ok {
				result.Operation.Value = val
			} else {
				result.Error = ErrNoKey
			}
		}
	case "Put":
		{
			if !kv.isDuplicated(op) {
				kv.data[op.Key] = op.Value
			}
		}
	case "Append":
		{
			if !kv.isDuplicated(op) {
				kv.data[op.Key] += op.Value
			}
		}
	}
	//DPrintf("server %d before!!! clientId is %d, requestId is %d, ack is %d", kv.me, op.ClientId, op.RequestId, kv.ack[op.ClientId])
	if !kv.isDuplicated(op) {
		kv.ack[op.ClientId] = op.RequestId
		//DPrintf("server %d after!!! clientId is %d, requestId is %d, ack is %d", kv.me, op.ClientId, op.RequestId, kv.ack[op.ClientId])
	}
	return result
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.CompareAndSwapInt32(&kv.isKilled, kv.isKilled, 1)
}

func (kv *KVServer) resetRequestChannel(index int) {
	if m, ok := kv.requestMap[index]; ok {
		select {
		case <-m:
		default:
		}
	} else {
		kv.requestMap[index] = make(chan OpResult, 1)
	}
}

func (kv *KVServer) run() {
	for atomic.LoadInt32(&kv.isKilled) == 0 {
		message := <-kv.applyCh
		kv.mu.Lock()
		if !message.CommandValid {
			// 此时说明raft已经应用了新的snapshot，server层也需要同步更新
			r := bytes.NewBuffer(message.Snapshot)
			d := labgob.NewDecoder(r)
			d.Decode(&kv.data)
			d.Decode(&kv.ack)
		} else {
			op, _ := message.Command.(Op)
			result := kv.applyOperation(op)
			// 需要从applych中抽取消息
			// 首先初始化channel
			kv.resetRequestChannel(message.CommandIndex)
			kv.requestMap[message.CommandIndex] <- result
			// 如果snapshot超过了size，需要存下来
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.data)
				e.Encode(kv.ack)
				go kv.rf.Snapshot(message.CommandIndex, w.Bytes())
			}
		}
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
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
	labgob.Register(OpResult{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.requestMap = make(map[int]chan OpResult)
	kv.data = make(map[string]string)
	kv.ack = make(map[int64]uint64)

	// You may need initialization code here.
	go kv.run()

	return kv
}
