package kvsrv

import (
	"6.5840/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	server  *labrpc.ClientEnd
	clerkId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.clerkId = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	rpcId := nrand()
	args := GetArgs{key, rpcId, ck.clerkId}
	var reply GetReply
	for !ck.server.Call("KVServer.Get", &args, &reply) {
		reply = GetReply{}
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	rpcId := nrand()
	args := PutAppendArgs{key, value, rpcId, ck.clerkId}
	var reply PutAppendReply
	for !ck.server.Call("KVServer."+op, &args, &reply) {
		reply = PutAppendReply{}
		// 这里一开始不小心赋值了value，导致服务器修改不了已经赋值的值，找了半天
		// 且每次尝试发送RPC都应该把reply值清空
	}
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	str := ck.PutAppend(key, value, "Append")
	return str
}
