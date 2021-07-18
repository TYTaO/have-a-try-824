package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	. "have-try-6.824/aboutTask/rpc"
	crash "have-try-6.824/aboutTask/test/crash"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

var ReduceOutNum int

func runWorker() {
	// get ReduceOutNum
	numReply := NumReply{}
	call("Master.GetReduceOutNum", &NoArgs{}, &numReply)
	ReduceOutNum = numReply.Num
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
		log.Println("get map task id: " + strconv.Itoa(reply.T.Id))
		rand.Seed(time.Now().UnixNano())
		taskNeedTime := rand.Intn(taskMaxTime)
		time.Sleep(time.Duration(taskNeedTime) * time.Second)

		// maptask: 写一个文件，填入一个map
		outFileNames := mapWork(task.Id, task.MapTaskFile)
		log.Printf("finish a map task %d\n", task.Id)

		// 告诉master完成
		finishTaskArgs := FinishTaskArgs{Id: task.Id, TaskKind: MAPTASK, TaskFiles: outFileNames}
		finishTaskReply := FinishTaskReply{}
		call("Master.FinishATask", &finishTaskArgs, &finishTaskReply)
	} else if reply.T.TaskKind == REDUCETASK {
		fmt.Println("get reduce task id: " + strconv.Itoa(reply.T.Id))
		rand.Seed(time.Now().UnixNano())
		taskNeedTime := rand.Intn(taskMaxTime)
		time.Sleep(time.Duration(taskNeedTime) * time.Second)
		// reducetask: 接着写一个文件，填入一个reduce
		reduceWork(reply.T.ReduceOutId, reply.T.ReduceTaskFiles)

		fmt.Printf("finish a reduce task %d\n", task.Id)

		// 告诉master完成
		finishTaskArgs := FinishTaskArgs{Id: task.Id, TaskKind: REDUCETASK}
		finishTaskReply := FinishTaskReply{}
		call("Master.FinishATask", &finishTaskArgs, &finishTaskReply)
	}
}

func mapWork(mapTaskId int, filename string) []string {
	log.Println("[check" + strconv.Itoa(mapTaskId) + "] start")
	intermediates := make([][]KeyValue, ReduceOutNum)
	for i := range intermediates {
		intermediates[i] = make([]KeyValue, 0)
	}
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	kva := crash.Map(filename, string(content))

	for _, kv := range kva {
		reduceId := ihash(kv.Key) % ReduceOutNum
		intermediates[reduceId] = append(intermediates[reduceId], kv)
	}
	outFilenames := make([]string, ReduceOutNum)

	for i := range outFilenames {
		// 写下map-out
		outFilenames[i] = "mr-" + strconv.Itoa(mapTaskId) + "-" + strconv.Itoa(i)
		outFile, err := os.Create(outFilenames[i])
		defer outFile.Close()
		if err != nil {
			log.Fatalf("cannot create %v", outFilenames[i])
		}
		enc := json.NewEncoder(outFile)
		for _, kv := range intermediates[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
	}

	log.Println("[check" + strconv.Itoa(mapTaskId) + "] end")
	return outFilenames
}

func reduceWork(reduceTaskId int, filenames []string) {
	intermediate := []KeyValue{}

	for _, filename := range filenames {
		// 读 map 中间 文件
		// read from map out file
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open: %v", err)
		}
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	// reduce
	sort.Sort(ByKey(intermediate))

	// use tempFile to ensure that nobody observes partially written files in the presence of crashes
	tempFile, err := ioutil.TempFile("./", "reduce-tmp-*")
	if err != nil {
		log.Fatalf("cannot create TempFile")
	}

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
		output := crash.Reduce(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	// On Windows we can't opera on tmp without closing it.
	tempFile.Close()
	reduceOutName := "mr-out-" + strconv.Itoa(reduceTaskId)
	err = os.Rename(tempFile.Name(), reduceOutName)
	if err != nil {
		fmt.Println(err)
		log.Fatalf("cannot Rename %v", tempFile.Name())
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func main() {
	workers := 3
	epoch := 10

	for i := 0; i < workers; i++ {
		go func() {
			//runWorker()
			for i := 0; i < epoch; i++ {
				runWorker()
			}
		}()
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
