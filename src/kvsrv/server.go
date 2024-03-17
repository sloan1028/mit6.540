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

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	key := args.Key
	reply.Value = kv.GetValue(key)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.PutValue(args.Key, args.Value)
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Value = kv.AppendValue(args.Key, args.Value)
	//fmt.Printf("reply.Value {%v}\n", reply.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.hashTable = make(map[string]string)
	// You may need initialization code here.

	return kv
}
