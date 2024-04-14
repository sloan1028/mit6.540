package kvraft

import (
	"6.5840/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId   int64
	leaderId  int
	commandId int
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
	ck.clerkId = nrand()
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	DPrintf("%d Call Get Key: %v\n", ck.clerkId, key)
	// You will have to modify this function.
	args := GetArgs{key, ck.commandId, ck.clerkId}
	for {
		reply := GetReply{}
		DPrintf("%d Call Get Key: %v\n", ck.clerkId, key)
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		//DPrintf("%d Receive value: %v\n", ck.clerkId, reply.Value)
		if !ok || reply.Err != OK {
			if reply.Err == ErrWrongLeader {
				DPrintf("Get ErrWrongLeader\n")
			} else if reply.Err == ErrTimeOut {
				DPrintf("Get ErrTimeOut\n")
			}
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else {
			DPrintf("Get success, commandId: %v\n", ck.commandId)
			ck.commandId++
			return reply.Value
		}
		if ck.leaderId == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{key, value, ck.commandId, ck.clerkId}
	for {
		reply := PutAppendReply{}
		DPrintf("%d Call %v Key: %v Value: %v, to Leader: %d\n", ck.clerkId, op, key, value, ck.leaderId)
		ok := ck.servers[ck.leaderId].Call("KVServer."+op, &args, &reply)
		if !ok || reply.Err != OK {
			if reply.Err == ErrWrongLeader {
				DPrintf("Get ErrWrongLeader\n")
			} else if reply.Err == ErrTimeOut {
				DPrintf("Get ErrTimeOut\n")
			}
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else {
			DPrintf("putappend success, commandId: %v\n", ck.commandId)
			ck.commandId++
			return
		}
		if ck.leaderId == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
