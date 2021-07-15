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
	rand.Seed(time.Now().UnixNano())
	taskNeedTime := rand.Intn(10)
	time.Sleep(time.Duration(taskNeedTime) * time.Second)
	fmt.Printf("finish a task %d\n", task.Id)

	// 告诉master完成
	finishTaskArgs := FinishTaskArgs{Id: task.Id}
	finishTaskReply := FinishTaskReply{}
	call("Master.FinishATask", &finishTaskArgs, &finishTaskReply)
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
	//runWorker()
	for i := 0; i < 10; i++ {
		go runWorker()
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
