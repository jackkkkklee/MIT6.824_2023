package mr

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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type phase int

const (
	// 0:map 1:reduce 2: done  reduce should be waiting util all map done
	MapPhase    phase = 0
	ReducePhase phase = 1
	DonePhase   phase = 2

	MapJob     int = 0
	ReduceJob  int = 1
	WaitingJob int = 2 //when map haven't been completed or all reduce job are assigned
	ExitJob    int = 3
)

type TaskArgs struct {
	WorkId int
}

type TaskReply struct {
	Task *Task
}

type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerId int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
