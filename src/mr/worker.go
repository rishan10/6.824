package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
workerLoop:
	for {
		task := GetTask()
		if task == nil {
			fmt.Println("Error contacting coordinator, exiting worker")
			break
		}

		switch v := (*task).(type) {
		case *MapTask:
			fmt.Println("Handling map task")
			handleMapTask(v, mapf)
		case *ReduceTask:
			fmt.Println("Handling reduce task")
			handleReduceTask(v, reducef)
		case *WaitTask:
			fmt.Println("Waiting for task")
			time.Sleep(100 * time.Millisecond)
		default:
			fmt.Println("Unknown task type")
			break workerLoop
		}
	}

}

func handleMapTask(task *MapTask, mapf func(string, string) []KeyValue) {
	filename := task.AbsFilePath
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	intermediate_files := make(map[int][]KeyValue)
	for _, kv := range kva {
		reduce_index := ihash(kv.Key) % task.NReduce
		intermediate_files[reduce_index] = append(intermediate_files[reduce_index], kv)
	}

	reduceTaskFileInfos := make(map[int][]string)

	for reduce_index, kva_pairs := range intermediate_files {
		filename := fmt.Sprintf("intermediate-%d-%d", task.MapTaskID, reduce_index)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("cannot create %v", filename)
		}
		enc := json.NewEncoder(file)
		enc.Encode(kva_pairs)
		reduceTaskFileInfos[reduce_index] = append(reduceTaskFileInfos[reduce_index], filename)
	}

	taskId := task.MapTaskID
	// call back to the coordinator to mark the task as complete
	CompleteMapTask(taskId, reduceTaskFileInfos)
}

func handleReduceTask(task *ReduceTask, reducef func(string, []string) string) {
	files := task.AbsIntermediateKVFilePaths
	intermediate := []KeyValue{}
	taskId := task.ReduceTaskID

	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		file_kvs := []KeyValue{}
		dec.Decode(&file_kvs)

		intermediate = append(intermediate, file_kvs...)
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", taskId)
	ofile, _ := os.Create(oname)
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
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	// call back to the coordinator to mark the task as complete
	CompleteReduceTask(taskId)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

func GetTask() *Task {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return reply.Task
	}
	return nil
}

func CompleteMapTask(taskId int, reduceTaskFileInfos map[int][]string) {
	// transform the infos into the RPC message struct
	reduceTaskFileInfosMsg := []ReduceTask{}
	for reduceIndex, filenames := range reduceTaskFileInfos {
		reduceTaskFileInfosMsg = append(reduceTaskFileInfosMsg, ReduceTask{AbsIntermediateKVFilePaths: filenames, ReduceTaskID: reduceIndex})
	}

	args := CompleteTaskArgs{TaskID: taskId, ReduceTaskFileInfos: reduceTaskFileInfosMsg}
	reply := CompleteTaskReply{}
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok {
		return
	}
	fmt.Println("Error completing task", taskId)
}

func CompleteReduceTask(taskId int) {
	args := CompleteTaskArgs{TaskID: taskId}
	reply := CompleteTaskReply{}
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok {
		return
	}
	fmt.Println("Error completing task", taskId)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
