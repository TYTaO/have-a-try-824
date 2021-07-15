package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"time"

	. "have-try-6.824/aboutTask/rpc"
)

func runWorker() {
	// declare an argument structure.
	args := TaskArgs{}

	// fill in the argument(s).

	// declare a reply structure.
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	fmt.Println("request a task")
	call("Master.DistributeTask", &args, &reply)
	task := reply.T
	if reply.T.Id == NOTASK || (reply.T.TaskKind != MAPTASK && reply.T.TaskKind != REDUCETASK) {
		fmt.Println("get no task")
		return
	}

	taskMaxTime := 2
	if reply.T.TaskKind == MAPTASK {
		fmt.Println("get map task")
		rand.Seed(time.Now().UnixNano())
		taskNeedTime := rand.Intn(taskMaxTime)
		time.Sleep(time.Duration(taskNeedTime) * time.Second)
		fmt.Printf("finish a map task %d\n", task.Id)

		// 告诉master完成
		finishTaskArgs := FinishTaskArgs{Id: task.Id, TaskKind: MAPTASK}
		finishTaskReply := FinishTaskReply{}
		call("Master.FinishATask", &finishTaskArgs, &finishTaskReply)
	} else if reply.T.TaskKind == REDUCETASK {
		fmt.Println("get reduce task")
		rand.Seed(time.Now().UnixNano())
		taskNeedTime := rand.Intn(taskMaxTime)
		time.Sleep(time.Duration(taskNeedTime) * time.Second)
		fmt.Printf("finish a reduce task %d\n", task.Id)

		// 告诉master完成
		finishTaskArgs := FinishTaskArgs{Id: task.Id, TaskKind: REDUCETASK}
		finishTaskReply := FinishTaskReply{}
		call("Master.FinishATask", &finishTaskArgs, &finishTaskReply)
	}
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	if err != nil {
		fmt.Println("master have exited")
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

func main() {
	workers := 3
	epoch := 10

	for i := 0; i < epoch; i++ {
		//runWorker()
		for i := 0; i < workers; i++ {
			rand.Seed(time.Now().UnixNano())
			waitTime := rand.Intn(400)
			time.Sleep(time.Duration(waitTime) * time.Millisecond)
			go runWorker()
		}
		time.Sleep(5 * time.Second)
	}

	args := NoArgs{}
	reply := NoReply{}
	for {
		isAlive := call("Master.IsAlive", &args, &reply)
		if !isAlive {
			break
		}
		time.Sleep(time.Second)
	}
}
