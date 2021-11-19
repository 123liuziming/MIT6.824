package kvraft

import (
	"6.824/labrpc"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	requestId uint64
	clientId  int64
	leaderId  int
}

func (ck *Clerk) increaseRequestId() {
	atomic.CompareAndSwapUint64(&ck.requestId, ck.requestId, ck.requestId+1)
}

func (ck *Clerk) getRequestId() uint64 {
	return atomic.LoadUint64(&ck.requestId)
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
	ck.requestId, ck.clientId, ck.leaderId = 0, nrand(), 0
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
	args, reply := GetArgs{Key: key, Base: BaseArgs{ClientId: ck.clientId, RequestId: ck.getRequestId()}}, GetReply{}
	ck.increaseRequestId()
	leader := ck.leaderId
	for ; ; leader = (leader + 1) % len(ck.servers) {
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			ck.leaderId = leader
			return reply.Value
		}
	}
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
	args, reply := PutAppendArgs{Key: key, Value: value, Op: op, Base: BaseArgs{ClientId: ck.clientId, RequestId: ck.getRequestId()}}, PutAppendReply{}
	ck.increaseRequestId()
	leader := ck.leaderId
	for ; ; leader = (leader + 1) % len(ck.servers) {
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			ck.leaderId = leader
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
