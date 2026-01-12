package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"encoding/gob"
	"os"
	"strconv"
)

// TASKS

type GetTaskArgs struct {
}

type GetTaskReply struct {
	Task *Task
}

type Task interface {
	IsMrTask()
}

type MapTask struct {
	AbsFilePath string
	NReduce     int
	MapTaskID   int
}

func (m *MapTask) IsMrTask() {}

type ReduceTask struct {
	AbsIntermediateKVFilePaths []string
	ReduceTaskID               int
}

func (r *ReduceTask) IsMrTask() {}

type WaitTask struct {
}

func (w *WaitTask) IsMrTask() {}

type CompleteTaskArgs struct {
	TaskID             int
	CreatedReduceTasks []*ReduceTask
}

type CompleteTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func init() {
	gob.Register(&MapTask{})
	gob.Register(&ReduceTask{})
	gob.Register(&WaitTask{})
}
