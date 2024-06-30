package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId  int64
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
	ck.clientId = nrand()
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &CommandRequest{
		ClientId: ck.clientId,
		Op:       Query,
		Num:      num,
	}
	// Your code here.
	for {
		reply := CommandResponse{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)
		if !ok || reply.Err != OK {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else {
			return reply.Config
		}
		if ck.leaderId == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &CommandRequest{
		CommandId: ck.commandId,
		ClientId:  ck.clientId,
		Op:        Join,
		Servers:   servers,
	}
	// Your code here.
	for {
		reply := CommandResponse{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		if !ok || reply.Err != OK {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else {
			ck.commandId++
			return
		}
		if ck.leaderId == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &CommandRequest{
		CommandId: ck.commandId,
		ClientId:  ck.clientId,
		Op:        Leave,
		GIDs:      gids,
	}
	// Your code here.
	for {
		reply := CommandResponse{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		if !ok || reply.Err != OK {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else {
			ck.commandId++
			return
		}
		if ck.leaderId == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &CommandRequest{
		CommandId: ck.commandId,
		ClientId:  ck.clientId,
		Op:        Move,
		Shard:     shard,
		GID:       gid,
	}
	// Your code here.
	for {
		reply := CommandResponse{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
		if !ok || reply.Err != OK {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else {
			ck.commandId++
			return
		}
		if ck.leaderId == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
}
