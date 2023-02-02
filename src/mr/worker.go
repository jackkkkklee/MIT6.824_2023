package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type WorkerStruct struct {
	WorkerId int
	Mapf     func(string, string) []KeyValue
	Reducef  func(string, []string) string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := WorkerStruct{
		Mapf:    mapf,
		Reducef: reducef,
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	w.Register()
	for {
		task := w.GetTask()
		fmt.Printf("assigned task: %v\n", task)
		switch task.JobType {
		case MapJob:
			w.DoMap(task)
			break
		case ReduceJob:
			w.DoReduce(task)
			break
		case WaitingJob:
			fmt.Printf("worker Id: %v  is sleeping\n", w.WorkerId)
			time.Sleep(time.Second)
			break
		case ExitJob:
			return
		default:
			fmt.Printf("ilegal job type\n")

		}
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
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

// GetTask get task from master by rpc
func (w *WorkerStruct) GetTask() *Task {
	args := TaskArgs{}
	reply := TaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	fmt.Printf("reply %v \n", reply.Task)
	if ok {
		return reply.Task
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}

func (w *WorkerStruct) Register() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		w.WorkerId = reply.WorkerId
	} else {
		fmt.Printf("call failed!\n")
	}
}

func (w *WorkerStruct) DoMap(task *Task) {
	//reduceId := ihash(key) % NReduce
	fmt.Printf("DoMap %v", *task)

	//code from mrseq
	intermediate := []KeyValue{}
	for _, filename := range task.Input {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := w.Mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	//hash intermediate to n temp reduce file
	//sort.Sort(ByKey(intermediate))
	kvFile := make([][]KeyValue, task.NumReduce)
	for _, v := range intermediate {
		index := ihash(v.Key) % task.NumReduce
		kvFile[index] = append(kvFile[index], v)
	}
	//output to local disk
	for i, v := range kvFile {
		oname := NamingTempFile(task.TaskId, i)
		f, _ := os.Create(oname)
		enc := json.NewEncoder(f)
		for _, kv := range v {
			enc.Encode(&kv)
		}
	}

	w.NotifyTaskDone(task)

}

func (w *WorkerStruct) DoReduce(task *Task) {
	fmt.Printf("DoReduce %v", task)
	files := task.Input
	kva := make([]KeyValue, 0)
	//read all related reduce temp file
	for _, fileName := range files {
		file, _ := os.Open(fileName)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	oname := NamingOutputFile(task.TaskId)
	//fmt.Printf("create output file %v\n", oname)
	ofile, _ := os.Create(oname)
	sort.Sort(ByKey(kva))
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := w.Reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
	w.NotifyTaskDone(task)
}

func (w *WorkerStruct) NotifyTaskDone(task *Task) {
	args := NotifyTaskDoneArgs{
		Jobholder: &JobHolder{
			Task:        task,
			JobWorkerId: w.WorkerId,
		},
	}
	reply := NotifyTaskDoneReply{}
	ok := call("Coordinator.NotifyTaskDone", &args, &reply)
	if ok {

	} else {
		fmt.Printf("call failed!\n")
	}
}

// NamingTempFile mr-x-y ;x is the number of map(default 8 based on *.txt),y is the number of reduce (default 10)
func NamingTempFile(mapId int, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
}

func NamingOutputFile(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
}
