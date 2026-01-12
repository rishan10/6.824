package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
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
			// fmt.Println("Handling map task")
			handleMapTask(v, mapf)
		case *ReduceTask:
			// fmt.Println("Handling reduce task")
			handleReduceTask(v, reducef)
		case *WaitTask:
			// fmt.Println("Waiting for task")
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

	curDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Error getting current directory: %v", err)
	}

	createdReduceTasks := []*ReduceTask{}
	for reduce_index, kva_pairs := range intermediate_files {
		filename := fmt.Sprintf("mr-%d-%d", task.MapTaskID, reduce_index)
		tmpFile, err := ioutil.TempFile("", "")
		defer os.Remove(tmpFile.Name())

		if err != nil {
			log.Fatalf("cannot create temp file")
		}

		enc := json.NewEncoder(tmpFile)
		enc.Encode(kva_pairs)

		os.Rename(tmpFile.Name(), filename)

		// Get absolute path for the created file
		absFilePath := filepath.Join(curDir, filename)
		createdReduceTasks = append(createdReduceTasks, &ReduceTask{
			ReduceTaskID:               reduce_index,
			AbsIntermediateKVFilePaths: []string{absFilePath},
		})
	}

	taskId := task.MapTaskID
	// call back to the coordinator to mark the task as complete
	CompleteMapTask(taskId, createdReduceTasks)
}

func handleReduceTask(task *ReduceTask, reducef func(string, []string) string) {
	files := task.AbsIntermediateKVFilePaths
	intermediate := []KeyValue{}
	taskId := task.ReduceTaskID

	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			fmt.Println("error opening file", filename, err)
			continue
		}
		dec := json.NewDecoder(file)
		file_kvs := []KeyValue{}
		dec.Decode(&file_kvs)

		intermediate = append(intermediate, file_kvs...)
	}

	if len(intermediate) == 0 {
		// This should not happen since we wont put empty tasks in the idle queue, but added a check for debugging
		fmt.Println("No intermediate files found for reduce task", taskId, "existing early")
		CompleteReduceTask(taskId)
		return
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", taskId)
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}

	defer os.Remove(tmpFile.Name())

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
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tmpFile.Close()
	os.Rename(tmpFile.Name(), oname)

	// call back to the coordinator to mark the task as complete
	CompleteReduceTask(taskId)
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

func CompleteMapTask(taskId int, createdReduceTasks []*ReduceTask) {
	args := CompleteTaskArgs{
		TaskID:             taskId,
		CreatedReduceTasks: createdReduceTasks,
	}
	reply := CompleteTaskReply{}
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok {
		return
	}
	fmt.Println("Error completing map task", taskId)
}

func CompleteReduceTask(taskId int) {
	args := CompleteTaskArgs{
		TaskID:             taskId,
		CreatedReduceTasks: nil,
	}
	reply := CompleteTaskReply{}
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok {
		return
	}
	fmt.Println("Error completing reduce task", taskId)
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
