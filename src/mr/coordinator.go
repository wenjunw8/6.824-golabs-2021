package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nReduce              int
	files                []string
	mapTasks             []*Task
	reduceTasks          []*Task
	completedMapTasks    int
	completedReduceTasks int
	mutex                sync.Mutex
}

type Task struct {
	taskID    int
	inputFile string
	state     int // 0: idle 1: in-progress 2: completed
	workerID  int
	startTime int64
}

func (c *Coordinator) assignMapTask(args *int, reply *TaskDesc) bool {
	for _, task := range c.mapTasks {
		switch task.state {
		case 0:
			task.state = 1
			task.workerID = *args
			task.startTime = time.Now().Unix()
			reply.TaskType = 0
			reply.TaskID = task.taskID
			reply.InputFile = task.inputFile
			return true
		case 1:
			if time.Now().Unix()-task.startTime > 10 {
				task.startTime = time.Now().Unix()
				task.state = 1
				task.workerID = *args
				reply.TaskType = 0
				reply.TaskID = task.taskID
				reply.InputFile = task.inputFile
				return true
			}
		}
	}
	return false
}

func (c *Coordinator) assignReduceTask(args *int, reply *TaskDesc) bool {
	if c.completedReduceTasks >= len(c.reduceTasks) {
		return false
	}

	for _, task := range c.reduceTasks {
		switch task.state {
		case 0:
			task.state = 1
			task.workerID = *args
			task.startTime = time.Now().Unix()
			reply.TaskType = 1
			reply.TaskID = task.taskID
			return true
		case 1:
			if time.Now().Unix()-task.startTime > 10 {
				task.startTime = time.Now().Unix()
				task.state = 1
				task.workerID = *args
				reply.TaskType = 1
				reply.TaskID = task.taskID
				return true
			}
		}
	}
	return false
}

func (c *Coordinator) ReportTaskCompletion(args *TaskDesc, reply *int) error {
	c.mutex.Lock()
	if args.TaskType == 0 {
		c.mapTasks[args.TaskID].state = 2
	} else if args.TaskType == 1 {
		c.reduceTasks[args.TaskID].state = 2
	}
	c.mutex.Unlock()
	return nil
}

func mapTasksCompleted(c *Coordinator) bool {
	for _, task := range c.mapTasks {
		if task.state != 2 {
			return false
		}
	}
	return true
}

func reduceTasksCompleted(c *Coordinator) bool {
	for _, task := range c.reduceTasks {
		if task.state != 2 {
			return false
		}
	}
	return true
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *int, reply *TaskDesc) error {
	c.mutex.Lock()
	reply.NumMap = len(c.mapTasks)
	reply.NumReduce = c.nReduce
	if c.assignMapTask(args, reply) {
		c.mutex.Unlock()
		return nil
	}
	if !mapTasksCompleted(c) {
		reply.TaskType = 3
		c.mutex.Unlock()
		return nil
	}

	if c.assignReduceTask(args, reply) {
		c.mutex.Unlock()
		return nil
	}

	if !reduceTasksCompleted(c) {
		reply.TaskType = 3 // NoTask
		c.mutex.Unlock()
		return nil
	}

	reply.TaskType = 2 // ExitTask
	c.mutex.Unlock()
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	// Your code here.
	c.mutex.Lock()
	if reduceTasksCompleted(c) {
		c.mutex.Unlock()
		return true
	}
	c.mutex.Unlock()
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.completedMapTasks = 0
	c.completedReduceTasks = 0

	// create map tasks
	for i, file := range files {
		task := Task{
			taskID:    i,
			inputFile: file,
			state:     0,
			workerID:  -1,
			startTime: 0,
		}
		c.mapTasks = append(c.mapTasks, &task)
	}

	// create reduce tasks
	for i := 0; i < nReduce; i++ {
		task := Task{
			taskID:    i,
			inputFile: "",
			state:     0,
			workerID:  -1,
			startTime: 0,
		}
		c.reduceTasks = append(c.reduceTasks, &task)
	}

	// start the RPC server

	c.server()
	return &c
}
