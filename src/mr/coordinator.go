package mr

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	TaskPhase        phase
	TaskChan         chan *Task
	Files            []string //input files
	NumReduce        int
	TaskId           int
	WorkerId         int
	RestTask         int
	TaskJobHolderMap map[int]*JobHolder
}
type Task struct {
	JobType   int
	Input     []string
	TaskId    int
	NumReduce int
}

type JobHolder struct {
	Task        *Task
	JobWorkerId int
	StartTime   time.Time // tracking time for backup
}

var mux = sync.Mutex{}
var TaskJobHolderMux = sync.Mutex{}
var fileMux = sync.Mutex{}
var RestTaskCountMux = sync.Mutex{}

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
	return c.TaskPhase == DonePhase
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:            files,
		NumReduce:        nReduce,
		TaskChan:         make(chan *Task, len(files)),
		TaskPhase:        MapPhase,
		TaskJobHolderMap: map[int]*JobHolder{},
	}

	// Your code here.
	//create map task
	c.GenerateMapTask(files)
	c.RestTask = len(c.TaskChan)
	c.server()
	go c.monitoring()
	return &c
}

func (c *Coordinator) GenerateMapTask(files []string) {
	//fmt.Printf("before map, input file : %v", c.Files)
	for _, v := range files {
		t := Task{
			JobType:   MapJob,
			TaskId:    c.GenerateTaskId(),
			Input:     []string{v}, //each file as an input
			NumReduce: c.NumReduce,
		}
		c.TaskChan <- &t
	}
	//clean
	c.Files = make([]string, 0)
}

//todo
func (c *Coordinator) GenerateReduceTask() {
	//fmt.Printf("before reduce, input file : %v", c.Files)
	copyFile := make([]string, len(c.Files))
	copy(copyFile, c.Files)
	c.Files = make([]string, 0) //clean
	sort.Slice(copyFile, func(i, j int) bool {
		a := strings.Split(copyFile[i], "-")
		b := strings.Split(copyFile[j], "-")
		return a[len(a)-1] < b[len(b)-1]
	})

	input := []string{}
	//fmt.Printf("sorted reduce, input file : %v", copyFile)
	copyFile = append(copyFile, "-fake")
	for i := 0; i < len(copyFile)-1; i++ {
		cur := strings.Split(copyFile[i], "-")
		next := strings.Split(copyFile[i+1], "-")

		if cur[len(cur)-1] == next[len(next)-1] {
			input = append(input, copyFile[i])
		} else {
			input = append(input, copyFile[i])
			//fmt.Printf("input %v \n", input)
			t := Task{
				JobType:   ReduceJob,
				TaskId:    c.GenerateTaskId(),
				Input:     input, //each file as an input
				NumReduce: c.NumReduce,
			}
			input = []string{}
			c.TaskChan <- &t
		}
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
	//if phase is done,then assign exit job
	fmt.Printf("current phase : %v", c.TaskPhase)
	if c.TaskPhase == DonePhase {
		t = &Task{JobType: ExitJob, TaskId: 0}
	} else {
		select {
		case t = <-c.TaskChan:
			fmt.Println(t)
			break
		default:
			//no task available,waiting!
			t = &Task{JobType: WaitingJob, TaskId: 0}
		}
	}

	reply.Task = t
	if t.JobType == WaitingJob || t.JobType == ExitJob { // ignore waiting task
		return nil
	}

	TaskJobHolderMux.Lock()
	//update taskId workId map
	c.TaskJobHolderMap[t.TaskId] = &JobHolder{Task: t, StartTime: time.Now(), JobWorkerId: args.WorkId}
	TaskJobHolderMux.Unlock()

	return nil
}

func (c *Coordinator) NotifyTaskDone(args *NotifyTaskDoneArgs, reply *NotifyTaskDoneReply) error {
	t := args.Jobholder.Task
	c.modifyTaskJobHolderMap(t.TaskId)

	if t.JobType == MapJob {
		c.AddInputFile(t.TaskId) //generate reduce task input by the same naming rule
	}
	RestTaskCountMux.Lock()
	c.RestTask--
	fmt.Printf("rest number of task = %d\n", c.RestTask)
	RestTaskCountMux.Unlock()
	return nil
}

func (c *Coordinator) AddInputFile(taskId int) {
	fileMux.Lock()
	for i := 0; i < c.NumReduce; i++ {
		c.Files = append(c.Files, NamingTempFile(taskId, i))
	}
	fileMux.Unlock()
}

//check task status   tasks in the chan is waiting, tasks in the map is working ,otherwise they are completed
func (c *Coordinator) monitoring() {
	for {
		copyMap := map[int]*JobHolder{}
		TaskJobHolderMux.Lock()
		for k, v := range c.TaskJobHolderMap {
			copyMap[k] = v
		}
		TaskJobHolderMux.Unlock()

		for k, v := range copyMap {
			//10s is base deadline
			if time.Since(v.StartTime).Seconds() > 10*time.Second.Seconds() {
				c.BackUp(k)
			}
		}
		c.TryToNextPhase()

		time.Sleep(100 * time.Millisecond)
	}
}

// BackUp re put task into chan
func (c *Coordinator) BackUp(taskId int) {
	fmt.Printf("run back up [task id]: %d", taskId)
	c.TaskChan <- c.TaskJobHolderMap[taskId].Task
	c.modifyTaskJobHolderMap(taskId)
}

func (c *Coordinator) modifyTaskJobHolderMap(taskId int) {
	TaskJobHolderMux.Lock()
	if _, ok := c.TaskJobHolderMap[taskId]; ok {
		delete(c.TaskJobHolderMap, taskId)
	} else {
		fmt.Printf("unknow task\n")
	}
	TaskJobHolderMux.Unlock()

}

func (c *Coordinator) TryToNextPhase() {
	if c.RestTask == 0 && c.TaskPhase < DonePhase {
		c.TaskPhase++
		switch c.TaskPhase {
		case ReducePhase:
			c.RestTask = c.NumReduce //update restTask first ,otherwise it could be reduced when tasks be taken during the gap
			c.GenerateReduceTask()
			break
		case DonePhase:
			break
		default:
			fmt.Printf("unknow phase\n")
		}
	}
}
