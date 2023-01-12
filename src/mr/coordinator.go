package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	TaskPhase phase
	TaskChan  chan *Task
	Files     []string //input files
	NumReduce int
	TaskId    int
	WorkerId  int

	TaskWorkerMap map[int]int
}
type Task struct {
	JobType   int
	Input     []string
	TaskId    int
	NumReduce int
}

type JobHolder struct {
	task        *Task
	jobWorkerId int       // todo: how to distinguish workers?
	startTime   time.Time // tracking time for backup
}

var mux = sync.Mutex{}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:         files,
		NumReduce:     nReduce,
		TaskChan:      make(chan *Task, len(files)),
		TaskPhase:     MapPhase,
		TaskWorkerMap: map[int]int{},
	}

	// Your code here.
	//create map task
	c.GenerateMapTask(files) //todo try to make it asynchronous

	c.server()
	return &c
}

func (c *Coordinator) GenerateMapTask(files []string) {
	for _, v := range files {
		t := Task{
			JobType:   MapJob,
			TaskId:    c.GenerateTaskId(),
			Input:     []string{v}, //each file as an input
			NumReduce: c.NumReduce,
		}
		c.TaskChan <- &t
	}
}

func (c *Coordinator) GenerateTaskId() int {
	//Autoincrement ID
	mux.Lock()
	c.TaskId++
	mux.Unlock()
	return c.TaskId
}

func (c *Coordinator) GenerateWorkerId() int {
	//Autoincrement ID
	c.TaskId++
	return c.TaskId
}

func (c *Coordinator) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	c.WorkerId++
	reply = &RegisterReply{WorkerId: c.WorkerId}
	return nil
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	//args.WorkId
	var t *Task

	select {
	case t = <-c.TaskChan:
		fmt.Println(t)
		break
	default:
		//no task available,waiting!
		t = &Task{JobType: WaitingJob, TaskId: c.GenerateTaskId()}
	}

	//fmt.Printf("chan len  %v\n", len(c.TaskChan))
	if t.JobType == WaitingJob { // ignore waiting task
		return nil
	}

	reply.Task = t
	mux.Lock()
	//update taskId workId map
	c.TaskWorkerMap[t.TaskId] = args.WorkId
	mux.Unlock()
	return nil
}
