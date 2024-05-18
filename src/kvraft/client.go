package kvraft

import (
	"6.5840/labrpc"
	"crypto/rand"
	"math/big"
	"time"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	clerkId   int64
	leaderId  int
	commandId int64
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
	return ck.Command(key, "", GetOp)
}
func (ck *Clerk) Put(key string, value string) {
	ck.Command(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {
	ck.Command(key, value, AppendOp)
}

func (ck *Clerk) Command(key string, value string, op OpType) string {
	args := CommandRequest{
		Key:     key,
		Value:   value,
		Op:      op,
		ClerkId: ck.clerkId,
	}
	if op != GetOp {
		args.CommandId = ck.commandId
	}
	for {
		reply := CommandResponse{}
		Debug(dClient, "Client: %d Send A Command to %d, CommandID: %d Op: %v, Key: %v, Value: %v\n",
			ck.clerkId, ck.leaderId, args.CommandId, args.Op, args.Key, args.Value)
		ok := ck.servers[ck.leaderId].Call("KVServer.Command", &args, &reply)
		if !ok || reply.Err != OK {
			if reply.Err == ErrWrongLeader {

			} else if reply.Err == ErrTimeOut {

			}
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else {
			if op != GetOp {
				ck.commandId++
			}
			return reply.Value
		}
		if ck.leaderId == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
}
