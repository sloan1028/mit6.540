package mr

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type FileState struct {
	FileId    int
	FileName  string
	State     int //文件状态 0：未被分配 1：已分配未完成 2：已完成
	timeStamp int64
}

type ReduceState struct {
	State     int //文件状态 0：未被分配 1：已分配未完成 2：已完成
	timeStamp int64
}

type Coordinator struct {
	// 加锁保护共享资源
	mu sync.Mutex

	WorkStage JobType
	nReduce   int
	FileMapMu sync.Mutex
	FileMap   map[int]FileState

	// reduce的工作状态
	ReduceJobStateMu sync.Mutex
	ReduceJobState   map[int]ReduceState

	workCnt int32
}

func (c *Coordinator) IncreseWorkCnt() {
	atomic.AddInt32(&c.workCnt, 1)
}
func (c *Coordinator) GetWorkCnt() int {
	return int(atomic.LoadInt32(&c.workCnt))
}

func (c *Coordinator) SetReduceJobState(key int, value ReduceState) {
	c.ReduceJobStateMu.Lock()         // 在修改前加锁
	defer c.ReduceJobStateMu.Unlock() // 修改结束后解锁
	c.ReduceJobState[key] = value
}

func (c *Coordinator) GetReduceJobState() map[int]ReduceState {
	c.ReduceJobStateMu.Lock()         // 在读取前加锁
	defer c.ReduceJobStateMu.Unlock() // 读取结束后解锁
	res := make(map[int]ReduceState)
	for key, value := range c.ReduceJobState {
		res[key] = value
	}
	return res
}

func (c *Coordinator) SetFileMap(key int, value FileState) {
	c.FileMapMu.Lock()         // 在修改前加锁
	defer c.FileMapMu.Unlock() // 修改结束后解锁
	c.FileMap[key] = value
}

func (c *Coordinator) GetFileMap() map[int]FileState {
	c.FileMapMu.Lock()         // 在读取前加锁
	defer c.FileMapMu.Unlock() // 读取结束后解锁
	res := make(map[int]FileState)
	for key, value := range c.FileMap {
		res[key] = value
	}
	return res
}

// SetWorkStage 设置Coordinator的WorkStage值，加锁以保证并发安全
func (c *Coordinator) SetWorkStage(value JobType) {
	c.mu.Lock()         // 在修改前加锁
	defer c.mu.Unlock() // 修改结束后解锁
	c.WorkStage = value
}

// GetWorkStage 安全地获取Coordinator的WorkStage值
func (c *Coordinator) GetWorkStage() JobType {
	c.mu.Lock()         // 在读取前加锁
	defer c.mu.Unlock() // 读取结束后解锁
	return c.WorkStage
}

func (e *Coordinator) Error() string {
	return "Error"
}

func (c *Coordinator) WorkerAskForAJob(args *AskForJobArgs, reply *WorkerProperty) error {
	reply.Type = Wait
	reply.WorkId = c.GetWorkCnt()
	reply.NReduce = c.nReduce
	workStage := c.GetWorkStage()
	if workStage == MapJob {
		for key, file := range c.GetFileMap() {
			if file.State == 0 {
				reply.Type = MapJob
				reply.MapOrReduceId = file.FileId
				reply.MapFileName = file.FileName
				// 直接通过映射和键更新状态
				tempFile := file
				tempFile.State = 1
				tempFile.timeStamp = GetTimeStamp()
				c.SetFileMap(key, tempFile)
				c.IncreseWorkCnt()
				break
			}
		}
	} else if workStage == ReduceJob {
		reduceJobState := c.GetReduceJobState()
		for i := 0; i < c.nReduce; i++ {
			if reduceJobState[i].State == 0 {
				newJobState := ReduceState{1, GetTimeStamp()}
				c.SetReduceJobState(i, newJobState)
				reply.MapOrReduceId = i
				reply.Type = ReduceJob
				c.IncreseWorkCnt()
				break
			}
		}
	} else {
		reply.Type = Wait
	}
	return nil
}

func (c *Coordinator) MapJobDone(args *JobDoneArgs, reply *JobDoneReply) error {
	// map工作完成后，修改任务文件状态
	if newFileState, exists := c.GetFileMap()[args.Id]; exists {
		// 修改文件状态
		newFileState.State = 2 // 将状态设置为 2：完成
		// 将修改后的结构体重新赋值给映射
		c.SetFileMap(args.Id, newFileState)
	} else {
		fmt.Printf("%s 文件名不存在于映射中", args.Id)
	}

	allMapDone := true
	for _, file := range c.GetFileMap() {
		if file.State != 2 {
			allMapDone = false
			break
		}
	}
	if allMapDone {
		c.SetWorkStage(ReduceJob)
	}
	return nil
}

func (c *Coordinator) ReduceJobDone(args *JobDoneArgs, reply *JobDoneReply) error {
	// Reduce工作完成后，修改任务文件状态
	newJobState := ReduceState{2, GetTimeStamp()}
	c.SetReduceJobState(args.Id, newJobState)
	return nil
}

func (c *Coordinator) ListenWorkerState() {
	for {
		workStage := c.GetWorkStage()
		timeNow := GetTimeStamp()
		if workStage == MapJob {
			for key, file := range c.GetFileMap() {
				if file.State == 1 && timeNow-file.timeStamp > 10 {
					newFileState := FileState{
						FileId:    key,
						FileName:  file.FileName,
						State:     0,
						timeStamp: GetTimeStamp(),
					}
					c.SetFileMap(key, newFileState)
				}
			}
		} else if workStage == ReduceJob {
			for key, file := range c.GetReduceJobState() {
				if file.State == 1 && timeNow-file.timeStamp > 10 {
					newFileState := ReduceState{
						State:     0,
						timeStamp: GetTimeStamp(),
					}
					c.SetReduceJobState(key, newFileState)
				}
			}
		}
		time.Sleep(10 * time.Second)
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	allReduceDone := true

	reduceJobState := c.GetReduceJobState()
	for i := 0; i < c.nReduce; i++ {
		if reduceJobState[i].State != 2 {
			allReduceDone = false
			break
		}
	}
	if allReduceDone {
		fmt.Println("mapReduce success")
	}
	return allReduceDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		WorkStage:      MapJob,
		nReduce:        nReduce,
		ReduceJobState: make(map[int]ReduceState, nReduce),
		FileMap:        make(map[int]FileState),
	}
	for i, v := range files {
		fileState := FileState{i, v, 0, 0}
		c.SetFileMap(i, fileState)
	}
	c.server()
	go c.ListenWorkerState()
	return &c
}

func GetTimeStamp() int64 {
	now := time.Now()
	secs := now.Unix()
	return secs
}
