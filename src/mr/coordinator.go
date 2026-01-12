package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	TaskExpiryDurationSeconds = 10 * time.Second
)

type Coordinator struct {
	MapTasks    []Task
	ReduceTasks []Task

	IdleTasks    []int
	PendingTasks map[int]time.Time

	mu                 sync.RWMutex
	reduceTasksStarted atomic.Bool
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	taskId := c.removeIdleTask()

	if taskId == -1 {
		var task Task = &WaitTask{}
		reply.Task = &task
		return nil
	}

	var task Task
	if c.reduceTasksStarted.Load() {
		task = c.ReduceTasks[taskId]
	} else {
		task = c.MapTasks[taskId]
	}

	c.addPendingTask(taskId)
	reply.Task = &task
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.removePendingTask(args.TaskID)

	// If this is a map task completion, aggregate the reported intermediate file paths
	if len(args.CreatedReduceTasks) > 0 {
		c.mu.Lock()
		defer c.mu.Unlock()

		for _, reportedTask := range args.CreatedReduceTasks {
			reduceTaskID := reportedTask.ReduceTaskID
			// Find the corresponding ReduceTask in c.ReduceTasks
			for _, reduceTask := range c.ReduceTasks {
				if rt := reduceTask.(*ReduceTask); rt.ReduceTaskID == reduceTaskID {
					// Append the file paths from the reported task
					rt.AbsIntermediateKVFilePaths = append(rt.AbsIntermediateKVFilePaths, reportedTask.AbsIntermediateKVFilePaths...)
					// Deduplicate file paths for this reduce task before releasing the lock
					seen := make(map[string]bool)
					uniquePaths := []string{}
					for _, path := range rt.AbsIntermediateKVFilePaths {
						if !seen[path] {
							seen[path] = true
							uniquePaths = append(uniquePaths, path)
						}
					}
					rt.AbsIntermediateKVFilePaths = uniquePaths
					break
				}
			}
		}
	}

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
		if c.reduceTasksStarted.Load() {
			fmt.Println("All tasks completed")
			ret = true
		} else {
			fmt.Println("All map tasks completed, starting reduce tasks")
			c.reduceTasksStarted.Store(true)

			// start in a new goroutine to allow the Done() method to free the lock
			// this should only be called once, since
			go func() {
				c.mu.RLock()
				taskIds := []int{}
				for _, reduceTask := range c.ReduceTasks {
					if len(reduceTask.(*ReduceTask).AbsIntermediateKVFilePaths) == 0 {
						continue
					}
					taskIds = append(taskIds, reduceTask.(*ReduceTask).ReduceTaskID)
				}
				c.mu.RUnlock()

				c.addIdleTasks(taskIds)
			}()
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.initMapTasks(files, nReduce)
	c.initReduceTasks(nReduce)

	// start a cleanup thread to move expired tasks back to idle
	go func() {
		for {
			time.Sleep(TaskExpiryDurationSeconds)
			expiredTasks := c.getExpiredTasks()
			for _, taskID := range expiredTasks {
				fmt.Println("Moving expired task", taskID, "back to idle")
				c.removePendingTask(taskID)
				c.addIdleTasks([]int{taskID})
			}
		}
	}()

	c.server()
	return &c
}

func (c *Coordinator) initMapTasks(files []string, nReduce int) {
	for i, file := range files {
		c.MapTasks = append(c.MapTasks, &MapTask{
			AbsFilePath: file,
			NReduce:     nReduce,
			MapTaskID:   i,
		})
		c.addIdleTasks([]int{i})
	}
}

func (c *Coordinator) initReduceTasks(nReduce int) {
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, &ReduceTask{
			ReduceTaskID:               i,
			AbsIntermediateKVFilePaths: []string{},
		})
	}
}

func (c *Coordinator) addIdleTasks(taskIDs []int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, taskID := range taskIDs {
		c.IdleTasks = append(c.IdleTasks, taskID)
	}
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
