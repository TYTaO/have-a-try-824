package rpc

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// 定义通信中的变量
var (
	NOTASK int = -1

	GENERATED   int = 0
	DISTRIBUTED int = 1
	FINISHED    int = 2

	MAPTASK    = 12
	REDUCETASK = 13
)

type NoArgs struct {
}

type NoReply struct {
}

type Task struct {
	Id       int
	State    int // 0: generated   1: distributed   2: finished
	TaskKind int
	TaskFile string
}

type TaskArgs struct {
}

type TaskReply struct {
	T Task
}

type FinishTaskArgs struct {
	Id       int
	TaskKind int
	TaskFile string
}

type FinishTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "824-mr-"
	s = strconv.Itoa(os.Getuid())
	return s
}
