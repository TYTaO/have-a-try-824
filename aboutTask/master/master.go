package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	. "have-try-6.824/aboutTask/rpc"
)

type task struct {
	id int
}

type Master struct {
	// Your definitions here.
	remainTaskNum int
	mtx           sync.Mutex
}

func (m *Master) FinishATask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	m.mtx.Lock()
	m.remainTaskNum--
	fmt.Printf("task is finished task id: %d\n", args.Id)
	m.mtx.Unlock()

	return nil
}

func (m *Master) DistributeTask(args *TaskArgs, reply *TaskReply) error {
	// 分发任务
	randIntn := rand.Intn(10)
	if randIntn%2 == 0 {
		fmt.Printf("Distribute a task %d\n", 0)
		reply.T = Task{Id: 0}
	} else {
		fmt.Printf("Distribute a task %d\n", 1)
		reply.T = Task{Id: 1}
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

	// Your code here.
	// all tasks have finished
	if m.remainTaskNum <= 0 {
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
	m.remainTaskNum = nReduce
	// Your code here.

	m.server()
	return &m
}

// 通过rpc 给worker发任务
func main() {
	m := MakeMaster(10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}
	time.Sleep(time.Second)
}
