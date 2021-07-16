package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
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

		// maptask: 写一个文件，填入一个map
		outFileName := mapWork(task.Id, task.TaskFile)
		fmt.Printf("finish a map task %d\n", task.Id)

		// 告诉master完成
		finishTaskArgs := FinishTaskArgs{Id: task.Id, TaskKind: MAPTASK, TaskFile: outFileName}
		finishTaskReply := FinishTaskReply{}
		call("Master.FinishATask", &finishTaskArgs, &finishTaskReply)
	} else if reply.T.TaskKind == REDUCETASK {
		fmt.Println("get reduce task")
		rand.Seed(time.Now().UnixNano())
		taskNeedTime := rand.Intn(taskMaxTime)
		time.Sleep(time.Duration(taskNeedTime) * time.Second)

		// reducetask: 接着写一个文件，填入一个reduce
		reduceWork(reply.T.Id, reply.T.TaskFile)

		fmt.Printf("finish a reduce task %d\n", task.Id)

		// 告诉master完成
		finishTaskArgs := FinishTaskArgs{Id: task.Id, TaskKind: REDUCETASK}
		finishTaskReply := FinishTaskReply{}
		call("Master.FinishATask", &finishTaskArgs, &finishTaskReply)
	}
}

func mapWork(mapTaskId int, filename string) string {
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	kva := Map(string(content))
	intermediate = append(intermediate, kva...)

	// 写下map-out
	outFilename := "mr-map-out-" + strconv.Itoa(mapTaskId)
	outFile, err := os.Create(outFilename)
	defer outFile.Close()
	if err != nil {
		log.Fatalf("cannot create %v", outFilename)
	}
	enc := json.NewEncoder(outFile)
	for _, kv := range intermediate {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
		}
	}

	return outFilename
}

func reduceWork(reduceTaskId int, filename string) {
	// 读 map 中间 文件
	// read from map out file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	dec := json.NewDecoder(file)
	intermediate := []KeyValue{}
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}
	// reduce
	sort.Sort(ByKey(intermediate))

	reduceOutName := "mr-reduce-out-" + strconv.Itoa(reduceTaskId)
	ofile, _ := os.Create(reduceOutName)
	defer ofile.Close()
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
		output := Reduce(values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
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
