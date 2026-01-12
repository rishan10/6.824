package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskExpiryDurationSeconds = 10 * time.Second
)

type Coordinator struct {
	// Your definitions here.
	Tasks        []Task            // append only slice of Tasks
	IdleTasks    []int             // int corresponds to the index of the task in the Tasks slice
	PendingTasks map[int]time.Time // map[taskID]Processingtime
	mu           sync.RWMutex      // mutex for concurrent access to datastructures
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	taskId := c.removeIdleTask()

	if taskId == -1 {
		var task Task = &WaitTask{}
		reply.Task = &task
		return nil
	}

	task := c.Tasks[taskId]
	c.addPendingTask(taskId)
	reply.Task = &task
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.removePendingTask(args.TaskID)
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.PendingTasks) == 0 && len(c.IdleTasks) == 0 {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.initMapTasks(files, nReduce)

	// start a cleanup thread to move expired tasks back to idle
	go func() {
		for {
			time.Sleep(TaskExpiryDurationSeconds)
			expiredTasks := c.getExpiredTasks()
			for _, taskID := range expiredTasks {
				fmt.Println("Moving expired task", taskID, "back to idle")
				c.removePendingTask(taskID)
				c.addIdleTask(taskID)
			}
		}
	}()

	c.server()
	return &c
}

func (c *Coordinator) initMapTasks(files []string, nReduce int) {
	for i, file := range files {
		c.Tasks = append(c.Tasks, &MapTask{
			AbsFilePath: file,
			NReduce:     nReduce,
			MapTaskID:   i,
		})
		c.addIdleTask(i)
	}
}

func (c *Coordinator) addIdleTask(taskID int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.IdleTasks = append(c.IdleTasks, taskID)
}

func (c *Coordinator) removeIdleTask() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.IdleTasks) == 0 {
		return -1
	}

	taskID := c.IdleTasks[0]
	c.IdleTasks = c.IdleTasks[1:]
	return taskID
}

func (c *Coordinator) addPendingTask(taskID int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.PendingTasks == nil {
		c.PendingTasks = make(map[int]time.Time)
	}
	c.PendingTasks[taskID] = time.Now()
}

func (c *Coordinator) removePendingTask(taskID int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.PendingTasks, taskID)
}

func (c *Coordinator) getExpiredTasks() []int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	expiredTasks := []int{}
	now := time.Now()

	for taskID, processingTime := range c.PendingTasks {
		if now.Sub(processingTime) > TaskExpiryDurationSeconds {
			expiredTasks = append(expiredTasks, taskID)
		}
	}
	return expiredTasks
}
