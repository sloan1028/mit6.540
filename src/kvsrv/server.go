package kvsrv

import (
	"log"
	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	hashTable map[string]string

	clerkId2rpcId sync.Map
	doneCache     sync.Map
	// Your definitions here.
}

func (kv *KVServer) GetValue(key string) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//fmt.Printf("Get key{%v} value{%v}\n", key, kv.hashTable[key])
	return kv.hashTable[key]
}

func (kv *KVServer) PutValue(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//fmt.Printf("Put key{%v} value{%v}\n", key, value)
	kv.hashTable[key] = value
}

func (kv *KVServer) AppendValue(key, value string) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	str := kv.hashTable[key]
	kv.hashTable[key] += value
	//fmt.Printf("Append key{%v} oldValue{%v} newValue{%v}\n", key, str, kv.hashTable[key])
	return str
}

// get不关心是否重复调用的，没有调到一直调用就是了
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	/*
		if value, isDone := kv.CheckOpIsDone(args.RpcId); isDone {
			reply.Value = value
			return
		}
	*/
	key := args.Key
	reply.Value = kv.GetValue(key)
	//kv.DoCacheAndRelease(args.ClerkId, args.RpcId, reply.Value)
}

// put只关心是不是调用到了，不关心返回值，所以不用存doneCache
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	if value, isDone := kv.CheckOpIsDone(args.RpcId); isDone {
		reply.Value = value
		return
	}
	kv.PutValue(args.Key, args.Value)
	reply.Value = args.Value
	kv.doneCache.Store(args.RpcId, "kfc") //存下哈希表知道这个操作完成了就行了
	//kv.DoCacheAndRelease(args.ClerkId, args.RpcId, reply.Value)
}

// append关心是不是调用到了，关心返回值，所以存doneCache
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if value, isDone := kv.CheckOpIsDone(args.RpcId); isDone {
		reply.Value = value
		return
	}
	reply.Value = kv.AppendValue(args.Key, args.Value)

	kv.DoCacheAndRelease(args.ClerkId, args.RpcId, reply.Value)
}

func (kv *KVServer) CheckOpIsDone(rpcId int64) (string, bool) {
	value, isDone := kv.doneCache.Load(rpcId)
	if !isDone {
		return "", isDone
	}
	return value.(string), isDone
}

// 当前存在clerk先前的rpc请求缓存就把他删掉
// 然后存储clerk现在对应的rpc请求，以及rpc请求对应的键值
func (kv *KVServer) DoCacheAndRelease(clerkId, rpcId int64, value string) {
	preRpcId, exist := kv.clerkId2rpcId.Load(clerkId)
	if exist {
		//fmt.Println("DoDelete")
		kv.doneCache.Delete(preRpcId)
	}
	kv.clerkId2rpcId.Store(clerkId, rpcId)
	kv.doneCache.Store(rpcId, value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.hashTable = make(map[string]string)
	// You may need initialization code here.

	return kv
}
