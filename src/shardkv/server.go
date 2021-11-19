package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardmaster"
	"bytes"
	"log"
	"sync"
	"time"
)

const Debug = 1

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
	Command string
	// GET, PUT, APPEND需要
	ClientId  int64
	RequestId int64
	Key       string
	Value     string
	// 更新配置需要
	Config shardmaster.Config
	Data   [shardmaster.NShards]map[string]string
	Ack    map[int64]int64
	// 清理shard需要
	ConfigNum int
	ShardId   int
}

type OpResult struct {
	Command     string
	OK          bool
	Err         Err
	// GET, PUT, APPEND需要
	ClientId  int64
	RequestId int64
	Value     string
	// 更新配置需要
	ConfigNum int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data     [shardmaster.NShards]map[string]string
	ack        map[int64]int64
	requestMap map[int]chan OpResult
	config     shardmaster.Config
	mck      *shardmaster.Clerk
}

//
// try to append the entry to raft servers' log and return OpResult.
// OpResult is valid if raft servers apply this entry before timeout.
//
func (kv *ShardKV) startCommand(entry Op) OpResult {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return OpResult{OK: false}
	}

	kv.mu.Lock()
	if _, ok := kv.requestMap[index]; !ok {
		kv.requestMap[index] = make(chan OpResult, 1)
	}
	kv.mu.Unlock()

	select {
	case result := <-kv.requestMap[index]:
		if isMatch(entry, result) {
			return result
		}
		return OpResult{OK: false}
	case <-time.After(240 * time.Millisecond):
		return OpResult{OK: false}
	}
}

//
// check if the OpResult corresponds to the log entry.
//
func isMatch(entry Op, OpResult OpResult) bool {
	if entry.Command == "reconfigure" {
		return entry.Config.Num == OpResult.ConfigNum
	}
	if entry.Command == "delete" {
		return entry.ConfigNum == OpResult.ConfigNum
	}
	return entry.ClientId == OpResult.ClientId && entry.RequestId == OpResult.RequestId
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	entry := Op{Command: "get", ClientId: args.ClientId, RequestId: args.RequestId, Key: args.Key}

	result := kv.startCommand(entry)
	if !result.OK {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err, reply.Value = result.Err, result.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	entry := Op{Command: args.Op, ClientId: args.ClientId, RequestId: args.RequestId, Key: args.Key, Value: args.Value}

	result := kv.startCommand(entry)
	if !result.OK {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = result.Err
}

func (kv *ShardKV) applyOperation(op Op) OpResult {
	result := OpResult{Command: op.Command, OK: true, Err: OK, ClientId: op.ClientId, RequestId: op.RequestId}

	switch op.Command {
	case "put":
		{
			kv.applyPut(op, &result)
		}
	case "append":
		{
			kv.applyAppend(op, &result)
		}
	case "get":
		{
			kv.applyGet(op, &result)
		}
	case "reconfigure":
		{
			kv.applyReconfigure(op)
		}
	case "delete":
		{
			kv.applyCleanup(op)
		}
	}
	return result
}

func (kv *ShardKV) applyPut(args Op, OpResult *OpResult) {
	if !kv.isValidKey(args.Key) {
		OpResult.Err = ErrWrongGroup
		return
	}
	if !kv.isDuplicated(args) {
		kv.data[key2shard(args.Key)][args.Key] = args.Value
		kv.ack[args.ClientId] = args.RequestId
	}
	OpResult.Err = OK
}

func (kv *ShardKV) applyAppend(args Op, OpResult *OpResult) {
	if !kv.isValidKey(args.Key) {
		OpResult.Err = ErrWrongGroup
		return
	}
	if !kv.isDuplicated(args) {
		kv.data[key2shard(args.Key)][args.Key] += args.Value
		kv.ack[args.ClientId] = args.RequestId
	}
	OpResult.Err = OK
}

func (kv *ShardKV) applyGet(args Op, OpResult *OpResult) {
	if !kv.isValidKey(args.Key) {
		OpResult.Err = ErrWrongGroup
		return
	}
	if !kv.isDuplicated(args) {
		kv.ack[args.ClientId] = args.RequestId
	}
	if value, ok := kv.data[key2shard(args.Key)][args.Key]; ok {
		OpResult.Value = value
		OpResult.Err = OK
	} else {
		OpResult.Err = ErrNoKey
	}
}

func (kv *ShardKV) applyReconfigure(args Op) {
	// 记得只能apply此config的编号+1的信config
	if args.Config.Num == kv.config.Num + 1 {
		// 更新数据
		for shardId, data := range args.Data {
			for k, v := range data {
				kv.data[shardId][k] = v
			}
		}
		// 合并ack map
		for k, v := range args.Ack {
			kv.updateAck(kv.ack, k, v)
		}
		// 更新现在的config
		lastConfig := kv.config
		kv.config = args.Config
		// 要求给其发送过数据的server删除不必要的shard
		for shardId, shardData := range args.Data {
			if len(shardData) > 0 {
				gid := lastConfig.Shards[shardId]
				deleteShardArgs := DeleteShardArgs{Num: lastConfig.Num, ShardId: shardId}
				go kv.sendDeleteShard(gid, &lastConfig, &deleteShardArgs, &DeleteShardReply{})
			}
		}
	}
}

func (kv *ShardKV) applyCleanup(args Op) {
	// 只能接收以前版本config的清理
	if args.ConfigNum > kv.config.Num {
		return
	}
	if kv.config.Shards[args.ShardId] != kv.gid {
		kv.data[args.ShardId] = make(map[string]string)
	}
}

//
// check if the request is duplicated with request id.
//
func (kv *ShardKV) isDuplicated(op Op) bool {
	lastRequestId, ok := kv.ack[op.ClientId]
	return ok && lastRequestId >= op.RequestId
}

//
// check if the key is handled by this replica group.
//
func (kv *ShardKV) isValidKey(key string) bool {
	return kv.config.Shards[key2shard(key)] == kv.gid
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) Run() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if !msg.CommandValid {
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)
			d.Decode(&kv.data)
			d.Decode(&kv.ack)
			d.Decode(&kv.config)
		} else {
			// apply operation and send OpResult
			op := msg.Command.(Op)
			result := kv.applyOperation(op)
			if ch, ok := kv.requestMap[msg.CommandIndex]; ok {
				select {
				case <-ch: // drain bad data
				default:
				}
			} else {
				kv.requestMap[msg.CommandIndex] = make(chan OpResult, 1)
			}
			kv.requestMap[msg.CommandIndex] <- result

			// create snapshot if raft state exceeds allowed size
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.data)
				e.Encode(kv.ack)
				e.Encode(kv.config)
				go kv.rf.Snapshot(msg.CommandIndex, w.Bytes())
			}
		}
		kv.mu.Unlock()
	}
}

type TransferShardArgs struct {
	Num      int
	ShardIds []int
}

type TransferShardReply struct {
	Err  Err
	Data [shardmaster.NShards]map[string]string
	Ack  map[int64]int64
}

// 传来的参数 1. 配置index； 2. 请求的shard
func (kv *ShardKV) TransferShard(args *TransferShardArgs, reply *TransferShardReply) {
	// 加锁
	// 判断配置号
	// 返回data和ack
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Num > kv.config.Num {
		reply.Err = ErrNotReady
		return
	}
	for i := 0; i < shardmaster.NShards; i++ {
		reply.Data[i] = make(map[string]string)
	}
	reply.Ack = make(map[int64]int64)
	for _, shardId := range args.ShardIds {
		for k, v := range kv.data[shardId] {
			reply.Data[shardId][k] = v
		}
		for k, v := range kv.ack {
			kv.updateAck(reply.Ack, k, v)
		}
	}
	reply.Err = OK
}

//
// try to get shards requested in args and ack from replica group specified by gid.
//
func (kv *ShardKV) sendTransferShard(gid int, args *TransferShardArgs, reply *TransferShardReply) bool {
	// 给group中所有的服务器都发TransferShard RPC
	notReadyFlag := true
	for _, server := range kv.config.Groups[gid] {
		end := kv.make_end(server)
		ok := end.Call("ShardKV.TransferShard", args, reply)
		if ok {
			if reply.Err == OK {
				return true
			}
			if reply.Err == ErrNotReady {
				notReadyFlag = false
			}
		}
	}
	return notReadyFlag
}

type ReconfigureArgs struct {
	Config shardmaster.Config
	Data   [shardmaster.NShards]map[string]string
	Ack    map[int64]int64
}

func (kv *ShardKV) updateAck(ack map[int64]int64, key int64, value int64) {
	requestId, ok := ack[key]
	if !ok || requestId < value {
		ack[key] = value
	}
}

func (kv *ShardKV) getReconfigureOp(nextConfig shardmaster.Config) (Op, bool) {
	op, ok := Op{Command: "reconfigure", Config: nextConfig}, true
	// 给所有的gid请求data，放入Op中返回
	shardsToTransfer := kv.getShardsToTransfer(nextConfig)
	for i := 0; i < shardmaster.NShards; i++ {
		op.Data[i] = make(map[string]string)
	}
	op.Ack = make(map[int64]int64)
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	for gid, shards := range shardsToTransfer {
		wg.Add(1)
		go func(gid int, shards []int) {
			defer wg.Done()
			transArgs, transReply := TransferShardArgs{Num: nextConfig.Num, ShardIds: shards}, TransferShardReply{}
			if kv.sendTransferShard(gid, &transArgs, &transReply) {
				if !ok {
					return
				}
				// 保证更新op相关数据的时候是原子更新
				mu.Lock()
				// 复制reply的数据至op中
				for _, shardId := range transArgs.ShardIds {
					for k, v := range transReply.Data[shardId] {
						op.Data[shardId][k] = v
					}
				}
				// 合并ack
				for k, v := range transReply.Ack {
					kv.updateAck(op.Ack, k, v)
				}
				mu.Unlock()
			} else {
				ok = false
			}
		}(gid, shards)
	}
	wg.Wait()
	return op, ok
}

//
// build a map from gid to shard ids to request from replica group specified by gid.
//
func (kv *ShardKV) getShardsToTransfer(nextConfig shardmaster.Config) map[int][]int {
	// 找所有需要从别的group拿的数据
	shardsToTransfer := make(map[int][]int)
	for i := 0; i < shardmaster.NShards; i++ {
		if kv.gid != kv.config.Shards[i] && nextConfig.Shards[i] == kv.gid {
			newGid := kv.config.Shards[i]
			if newGid != 0 {
				if _, ok := shardsToTransfer[newGid]; !ok {
					shardsToTransfer[newGid] = make([]int, 0)
				}
				shardsToTransfer[newGid] = append(shardsToTransfer[newGid], i)
			}
		}
	}
	return shardsToTransfer
}

//
// if this server is leader of the replica group , it should query latest
// configuration periodically and try to update configuration if rquired.
//
func (kv *ShardKV) PullNewConfig() {
	for {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			latestConfig := kv.mck.Query(-1)
			for i := kv.config.Num + 1; i <= latestConfig.Num; i++ {
				newConfig := kv.mck.Query(i)
				op, ok := kv.getReconfigureOp(newConfig)
				if !ok {
					break
				}
				result := kv.startCommand(op)
				if !result.OK {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

type DeleteShardArgs struct {
	Num     int
	ShardId int
}

type DeleteShardReply struct {
	Err         Err
}

//
// if this server is ready, create an entry to delete the shard in args and try
// to append it to raft servers' log.
//
func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if args.Num > kv.config.Num {
		reply.Err = ErrNotReady
	}
	op := Op{Command: "delete", ConfigNum: args.Num, ShardId: args.ShardId}
	kv.startCommand(op)
	reply.Err = OK
}

//
// try to ask replica group specified by gid in last config to delete the shard
// no longer required.
//
func (kv *ShardKV) sendDeleteShard(gid int, lastConfig *shardmaster.Config, args *DeleteShardArgs, reply *DeleteShardReply) bool {
	for _, server := range lastConfig.Groups[gid] {
		end := kv.make_end(server)
		ok := end.Call("ShardKV.DeleteShard", args, reply)
		if ok && reply.Err == OK {
			return true
		}
	}
	return false
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(OpResult{})
	labgob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	for i := 0; i < shardmaster.NShards; i++ {
		kv.data[i] = make(map[string]string)
	}
	kv.ack = make(map[int64]int64)
	kv.requestMap = make(map[int]chan OpResult)
	kv.mck = shardmaster.MakeClerk(masters)

	go kv.Run()
	go kv.PullNewConfig()

	return kv
}
