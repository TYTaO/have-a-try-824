package main

import (
	"fmt"
	"hash/fnv"
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
	ReduceOutNum int // do not confuse nReduce to ReduceOutNum
	nReduce      int
	nMap         int
	mtx          sync.Mutex

	mapTasks               []Task
	reduceTasks            []Task
	reduceTaskFileLists    [][]string
	hasGenerateReduceTasks bool
}

func (m *Master) FinishATask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	m.mtx.Lock()
	if args.TaskKind == MAPTASK {
		if m.mapTasks[args.Id].State == DISTRIBUTED {
			m.mapTasks[args.Id].State = FINISHED
			fmt.Printf("map task is finished task id: %d\n", args.Id)
			mapFinishedChans[args.Id] <- true
			m.nMap--
			// generate ReduceOutNum 个 ReduceTaskFiles of reduce task
			for i, _ := range args.TaskFiles {
				if len(args.TaskFiles) != m.ReduceOutNum {
					log.Fatalln("len(args.TaskFiles) != m.ReduceOutNum")
				}
				m.reduceTaskFileLists[i] = append(m.reduceTaskFileLists[i], args.TaskFiles[i])
			}
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

func (m *Master) GetReduceOutNum(args *NoArgs, reply *NumReply) error {
	reply.Num = m.ReduceOutNum

	return nil
}

// WaitMapTaskFinished
// reduces can't start until the last map has finished.
func (m *Master) WaitMapTaskFinished(args *NoArgs, reply *WaitMapReply) error {
	reply.IsFinished = m.nMap <= 0
	return nil
}

// go waitTaskFinished(id)
func waitTaskFinished(taskId int, taskKind int) {
	if taskKind == MAPTASK {
		select {
		// 设计一个超时
		case <-time.After(10 * time.Second):
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
	// 分发任务
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
	m.mtx.Unlock()
	// wait map task 过程就别占着锁了！！！
	if hasDistribute {
		return nil
	}
	// wait map task finished
	for true {
		if m.nMap <= 0 {
			break
		}
		fmt.Println("wait map tasks finished")
		time.Sleep(time.Second)
	}
	m.mtx.Lock()
	// generate reduce tasks
	if !m.hasGenerateReduceTasks {
		if !m.hasGenerateReduceTasks {
			for i, _ := range m.reduceTaskFileLists {
				// generate reduce task
				atomic.AddInt32(&redTaskCounter, 1)
				m.reduceTasks = append(m.reduceTasks, Task{Id: int(redTaskCounter),
					State: GENERATED, TaskKind: REDUCETASK, ReduceOutId: i, ReduceTaskFiles: m.reduceTaskFileLists[i]})
				m.nReduce++
			}
			m.hasGenerateReduceTasks = true
		}
	}

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
	if !hasDistribute {
		// 必须显式说明，不然默认为0了
		reply.T = Task{Id: NOTASK}
	}
	m.mtx.Unlock()
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
	fmt.Printf("nReduce: %d, nMap: %d\n", m.nReduce, m.nMap)
	// Your code here.
	// all tasks have finished
	if m.hasGenerateReduceTasks && m.nReduce <= 0 && m.nMap <= 0 {
		ret = true
		fmt.Println("The job has finished!")
	}
	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nMap = len(files)
	m.ReduceOutNum = nReduce
	m.nReduce = 0
	// Your code here.
	// init task
	m.mapTasks = make([]Task, 0)
	m.reduceTasks = make([]Task, 0)
	m.reduceTaskFileLists = make([][]string, m.ReduceOutNum)
	m.hasGenerateReduceTasks = false
	initMapTaskNum := len(files)
	for i := 0; i < initMapTaskNum; i++ {
		m.mapTasks = append(m.mapTasks, Task{Id: i, State: GENERATED, TaskKind: MAPTASK, MapTaskFile: files[i]})
	}

	m.server()
	return &m
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var m *Master
var mapFinishedChans []chan bool // 用来传输任务已完成
var redFinishedChans []chan bool // 用来传输任务已完成
var redTaskCounter int32
var logger *log.Logger

// 通过rpc 给worker发任务
func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}
	tasks := len(os.Args[1:])
	fmt.Printf("mapreduce files: %v\n", os.Args[1:])
	// need args to indicate the file to word count
	ReduceOutNum := 10 // 6.824 里是nReduce
	m = MakeMaster(os.Args[1:], ReduceOutNum)
	redTaskCounter = -1
	mapFinishedChans = make([]chan bool, 0)
	for i := 0; i < tasks; i++ {
		mapFinishedChans = append(mapFinishedChans, make(chan bool, 2))
	}
	redFinishedChans = make([]chan bool, 0) // 之后每生产一个reduce task，加一个
	for i := 0; i < m.ReduceOutNum; i++ {
		redFinishedChans = append(redFinishedChans, make(chan bool, 2))
	}
	// log
	file := "masterLog" + ".txt"
	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		panic(err)
	}
	logger = log.New(logFile, "[master]", log.LstdFlags|log.Lshortfile|log.LUTC)

	for m.Done() == false {
		time.Sleep(2 * time.Second)
	}
	time.Sleep(time.Second)
}
