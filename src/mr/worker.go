package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		workProperty := AskForAJob()

		switch workProperty.Type {
		case MapJob:
			DoMapJob(&workProperty, mapf)
		case ReduceJob:
			DoReduceJob(&workProperty, reducef)
		case Wait:
			time.Sleep(time.Second)
			// 处理等待情况，如果没有需要执行的特定操作，可以留空
		case CompleteJob:
			os.Exit(0)
		default:
			// 可选：处理未知的JobType
			panic(fmt.Sprintf("unknow JobType\n"))
		}
	}
}

func AskForAJob() WorkerProperty {
	// declare an argument structure.
	args := AskForJobArgs{}

	// declare a reply structure.
	reply := WorkerProperty{}

	ok := call("Coordinator.WorkerAskForAJob", &args, &reply)
	if ok {
		//fmt.Printf("AskForAJob success %v \n", reply)
	} else {
		fmt.Printf("AskForAJob failed! %v \n")
	}
	return reply
}

func DoMapJob(property *WorkerProperty, mapf func(string, string) []KeyValue) {
	filename := property.MapFileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := mapf(filename, string(content))
	// intermediate 是从一个文件里面读出来的，需要分配到nReduce个文件里面，
	// 分配后的名称为 mr-workId-ihash(key)

	// 创建nReduce个文件，名字为mr-workId-0...mr-workId-{nReduce-1}
	inputFiles := make([]*os.File, property.NReduce)
	inputEnc := make([]*json.Encoder, property.NReduce)
	for i := 0; i < property.NReduce; i++ {
		name := fmt.Sprintf("mr-%d-%d", property.WorkId, i)
		inputFile, _ := os.CreateTemp("", name+"*")
		inputFiles[i] = inputFile
		//fmt.Println(inputFile.Name())
		inputEnc[i] = json.NewEncoder(inputFile)
	}
	for _, kv := range intermediate {
		inputId := ihash(kv.Key) % property.NReduce
		err := inputEnc[inputId].Encode(&kv)
		if err != nil {
			fmt.Printf("EnCode Error %v \n", err)
		}
	}
	for i := 0; i < property.NReduce; i++ {
		name := fmt.Sprintf("mr-%d-%d", property.WorkId, i)
		oldName := inputFiles[i].Name()
		err := os.Rename(oldName, name)
		if err != nil {
			fmt.Println("重命名失败")
		}
	}

	args := &JobDoneArgs{property.MapOrReduceId}
	reply := &JobDoneReply{}
	// 写完之后发送个rpc给master，通知map工作结束了
	call("Coordinator.MapJobDone", &args, &reply)
}

func DoReduceJob(property *WorkerProperty, reducef func(string, []string) string) {
	intermediate := []KeyValue{}

	// 把需要的文件内容读出来
	suffixNum := property.MapOrReduceId
	pattern := fmt.Sprintf("mr-*-%d", suffixNum) // 使用变量构造模式
	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// 忽略目录，只处理文件
		if info.IsDir() {
			return nil
		}
		// 检查文件名是否匹配
		if matched, _ := filepath.Match(pattern, filepath.Base(path)); matched {
			file, err := os.Open(path)
			if err == nil {
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
		}
		return nil
	})
	if err != nil {
		fmt.Println("filepath.Walk Error:", err)
	}

	sort.Sort(ByKey(intermediate))
	name := fmt.Sprintf("mr-out-%d", property.MapOrReduceId) // 使用变量构造模式
	ofile, _ := os.CreateTemp("", name+"*")

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()

	oldName := ofile.Name()
	err = os.Rename(oldName, name)
	if err != nil {
		fmt.Println("Reduce重命名失败")
	}
	args := &JobDoneArgs{property.MapOrReduceId}
	reply := &JobDoneReply{}
	// 写完之后发送个rpc给master，通知reduce工作结束了
	call("Coordinator.ReduceJobDone", &args, &reply)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
