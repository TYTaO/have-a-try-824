package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"

	. "have-try-6.824/aboutTask/rpc"
)

type Master struct {
	// Your definitions here.
	nReduce int
	nMap    int
	mtx     sync.Mutex

	mapTasks    []Task
	reduceTasks []Task
}

func (m *Master) FinishATask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	m.mtx.Lock()
	if args.TaskKind == MAPTASK {
		if m.mapTasks[args.Id].State == DISTRIBUTED {
			m.mapTasks[args.Id].State = FINISHED
			fmt.Printf("map task is finished task id: %d\n", args.Id)
			mapFinishedChans[args.Id] <- true
			m.nMap--
			// generate reduce task
			atomic.AddInt32(&redTaskCounter, 1)
			m.reduceTasks = append(m.reduceTasks, Task{Id: int(redTaskCounter),
				State: GENERATED, TaskKind: REDUCETASK, TaskFile: args.TaskFile})
			m.nReduce++
		} else {
			fmt.Printf("map task is canceled task id: %d\n", args.Id)
		}
	} else if args.TaskKind == REDUCETASK {
		if m.reduceTasks[args.Id].State == DISTRIBUTED {
			m.reduceTasks[args.Id].State = FINISHED
			fmt.Printf("reduce task is finished task id: %d\n", args.Id)
			redFinishedChans[args.Id] <- true
			m.nReduce--
		} else {
			fmt.Printf("reduce task is canceled task id: %d\n", args.Id)
		}
	}
	m.mtx.Unlock()

	return nil
}

// go waitTaskFinished(id)
func waitTaskFinished(taskId int, taskKind int) {
	if taskKind == MAPTASK {
		select {
		// 设计一个超时
		case <-time.After(20 * time.Second):
			m.mtx.Lock()
			m.mapTasks[taskId].State = GENERATED
			fmt.Printf("map task is timeout task id: %d\n", taskId)
			m.mtx.Unlock()
		case <-mapFinishedChans[taskId]: // task finished
			//
		}
	} else if taskKind == REDUCETASK {
		select {
		// 设计一个超时
		case <-time.After(10 * time.Second):
			m.mtx.Lock()
			m.reduceTasks[taskId].State = GENERATED
			fmt.Printf("reduce task is timeout task id: %d\n", taskId)
			m.mtx.Unlock()
		case <-redFinishedChans[taskId]: // task finished
			//
		}
	}
}

// 给worker一个活着的信号，在worker需要关闭时起作用
func (m *Master) IsAlive(args *NoArgs, reply *NoReply) error {
	return nil
}

func (m *Master) DistributeTask(args *TaskArgs, reply *TaskReply) error {
	hasDistribute := false
	m.mtx.Lock()
	defer m.mtx.Unlock()
	// 分发任务
	// reduce 任务
	for i, _ := range m.reduceTasks {
		if m.reduceTasks[i].State == GENERATED {
			fmt.Printf("Distribute a reduce task %d\n", m.reduceTasks[i].Id)
			reply.T = m.reduceTasks[i]
			hasDistribute = true
			m.reduceTasks[i].State = DISTRIBUTED
			go waitTaskFinished(m.reduceTasks[i].Id, REDUCETASK)
			break
		}
	}
	if hasDistribute {
		return nil
	}
	// map 任务
	for i, _ := range m.mapTasks {
		if m.mapTasks[i].State == GENERATED {
			fmt.Printf("Distribute a map task %d\n", m.mapTasks[i].Id)
			reply.T = m.mapTasks[i]
			hasDistribute = true
			m.mapTasks[i].State = DISTRIBUTED
			go waitTaskFinished(m.mapTasks[i].Id, MAPTASK)
			break
		}
	}
	if !hasDistribute {
		// 必须显式说明，不然默认为0了
		reply.T = Task{Id: NOTASK}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	fmt.Printf("MapTasks: %v\n", m.mapTasks)
	fmt.Printf("RedTasks: %v\n", m.reduceTasks)
	// Your code here.
	// all tasks have finished
	if m.nReduce <= 0 && m.nMap <= 0 {
		ret = true
	}
	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nMap = nReduce
	// Your code here.
	// init task
	m.mapTasks = make([]Task, 0)
	m.reduceTasks = make([]Task, 0)
	initMapTaskNum := nReduce
	for i := 0; i < initMapTaskNum; i++ {
		m.mapTasks = append(m.mapTasks, Task{Id: i, State: GENERATED, TaskKind: MAPTASK, TaskFile: files[i]})
	}

	m.server()
	return &m
}

var m *Master
var mapFinishedChans []chan bool // 用来传输任务已完成
var redFinishedChans []chan bool // 用来传输任务已完成
var redTaskCounter int32

// 通过rpc 给worker发任务
func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}
	tasks := len(os.Args[1:])
	fmt.Printf("mapreduce files: %v\n", os.Args[1:])
	// need args to indicate the file to word count
	m = MakeMaster(os.Args[1:], tasks)
	redTaskCounter = -1
	mapFinishedChans = make([]chan bool, 0)
	for i := 0; i < tasks; i++ {
		mapFinishedChans = append(mapFinishedChans, make(chan bool, 2))
	}
	redFinishedChans = make([]chan bool, 0)
	for i := 0; i < tasks; i++ {
		redFinishedChans = append(redFinishedChans, make(chan bool, 2))
	}

	for m.Done() == false {
		time.Sleep(2 * time.Second)
	}
	time.Sleep(time.Second)
}
