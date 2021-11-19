package shardmaster

//
// Shardmaster clerk.
//

import (
	"6.824/labrpc"
	"sync"
	"sync/atomic"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	requestId int64
	clientId  int64
	mu        sync.Mutex
}

func (ck *Clerk) increaseReqId() {
	atomic.AddInt64(&ck.requestId, 1)
}

func (ck *Clerk) getReqId() int64 {
	return atomic.LoadInt64(&ck.requestId)
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
	ck.clientId, ck.requestId = nrand(), 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	ck.increaseReqId()
	args.Num, args.ClientId, args.RequestId = num, ck.clientId, ck.getReqId()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		//log.Println("query retry")
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	ck.increaseReqId()
	args.Servers, args.ClientId, args.RequestId = servers, ck.clientId, ck.getReqId()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	ck.increaseReqId()
	args.GIDs, args.ClientId, args.RequestId = gids, ck.clientId, ck.getReqId()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	ck.increaseReqId()
	args.Shard = shard
	args.GID = gid
	args.ClientId, args.RequestId = ck.clientId, ck.getReqId()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
