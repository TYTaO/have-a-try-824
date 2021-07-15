package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	. "have-try-6.824/aboutTask/rpc"
)

var (
	GENERATED   int = 0
	DISTRIBUTED int = 1
	FINISHED    int = 2

	NOTASK int = -1
)

type task struct {
	id    int
	state int // 0: generated   1: distributed   2: finished
}

type Master struct {
	// Your definitions here.
	nReduce int
	mtx     sync.Mutex

	tasks []task
}

func (m *Master) FinishATask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	m.mtx.Lock()
	if m.tasks[args.Id].state == DISTRIBUTED {
		m.tasks[args.Id].state = FINISHED
		fmt.Printf("task is finished task id: %d\n", args.Id)
		m.nReduce--
		chans[args.Id] <- true
	} else {
		fmt.Printf("task is canceled task id: %d\n", args.Id)
	}
	m.mtx.Unlock()

	return nil
}

// go waitTaskFinished(id)
func waitTaskFinished(taskId int) {
	select {
	// 设计一个超时
	case <-time.After(10 * time.Second):
		m.mtx.Lock()
		fmt.Println(m.tasks)
		m.tasks[taskId].state = GENERATED
		fmt.Printf("task is timeout task id: %d\n", taskId)
		m.mtx.Unlock()
	case <-chans[taskId]: // task finished
		//
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
	for i, _ := range m.tasks {
		if m.tasks[i].state == GENERATED {
			fmt.Printf("Distribute a task %d\n", m.tasks[i].id)
			reply.T = Task{Id: m.tasks[i].id}
			hasDistribute = true
			m.tasks[i].state = DISTRIBUTED
			go waitTaskFinished(m.tasks[i].id)
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
	fmt.Println(m.tasks)
	// Your code here.
	// all tasks have finished
	if m.nReduce <= 0 {
		ret = true
	}
	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(nReduce int) *Master {
	m := Master{}
	m.nReduce = nReduce
	// Your code here.
	// init task
	m.tasks = make([]task, 0)
	initTaskNum := nReduce
	for i := 0; i < initTaskNum; i++ {
		m.tasks = append(m.tasks, task{id: i, state: GENERATED})
	}

	m.server()
	return &m
}

var m *Master
var chans []chan bool // 用来传输任务已完成
// 通过rpc 给worker发任务
func main() {
	m = MakeMaster(10)
	chans = make([]chan bool, 0)
	for i := 0; i < 10; i++ {
		chans = append(chans, make(chan bool, 2))
	}

	for m.Done() == false {
		time.Sleep(2 * time.Second)
	}
	time.Sleep(time.Second)
}
