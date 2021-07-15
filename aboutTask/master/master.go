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
	m.tasks[args.Id].state = FINISHED
	fmt.Printf("task is finished task id: %d\n", args.Id)
	m.nReduce--
	m.mtx.Unlock()

	return nil
}

func (m *Master) DistributeTask(args *TaskArgs, reply *TaskReply) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	// 分发任务
	for i, _ := range m.tasks {
		if m.tasks[i].state == GENERATED {
			fmt.Printf("Distribute a task %d\n", m.tasks[i].id)
			reply.T = Task{Id: m.tasks[i].id}
			m.tasks[i].state = DISTRIBUTED
			break
		}
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

// 通过rpc 给worker发任务
func main() {
	m := MakeMaster(10)
	for m.Done() == false {
		time.Sleep(2 * time.Second)
	}
	time.Sleep(time.Second)
}
