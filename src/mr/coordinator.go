package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type Coordinator struct {
	// Your definitions here.
	nReduce     int
	nMap        int
	files       []string
	mapTasks    []*Task
	reduceTasks []*Task
	mutex       sync.Mutex
}

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Task struct {
	taskID    int
	inputFile string
	workerID  int
	startTime int64
	state     TaskState
}

func (c *Coordinator) getIdleTask(tasks []*Task) *Task {
	for _, task := range tasks {
		switch task.state {
		case Idle:
			return task
		case InProgress:
			if time.Now().Unix()-task.startTime > 10 {
				return task
			}
		}
	}
	return nil
}

func (c *Coordinator) ReportTaskCompletion(args *TaskDesc, reply *int) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if args.Type == MapTask {
		c.mapTasks[args.TaskID].state = Completed
	} else if args.Type == ReduceTask {
		c.reduceTasks[args.TaskID].state = Completed
	}
	return nil
}

func mapTasksCompleted(c *Coordinator) bool {
	for _, task := range c.mapTasks {
		if task.state != Completed {
			return false
		}
	}
	return true
}

func reduceTasksCompleted(c *Coordinator) bool {
	for _, task := range c.reduceTasks {
		if task.state != Completed {
			return false
		}
	}
	return true
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *int, reply *TaskDesc) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	reply.NumMap = c.nMap
	reply.NumReduce = c.nReduce

	task := c.getIdleTask(c.mapTasks)
	if task != nil {
		reply.Type = MapTask
		reply.TaskID = task.taskID
		reply.InputFile = task.inputFile

		task.state = InProgress
		task.workerID = *args
		task.startTime = time.Now().Unix()
		return nil
	}

	if !mapTasksCompleted(c) {
		reply.Type = WaitTask
		return nil
	}

	task = c.getIdleTask(c.reduceTasks)
	if task != nil {
		reply.Type = ReduceTask
		reply.TaskID = task.taskID

		task.state = InProgress
		task.workerID = *args
		task.startTime = time.Now().Unix()
		return nil
	}

	if !reduceTasksCompleted(c) {
		reply.Type = WaitTask
		return nil
	}

	reply.Type = ExitTask
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
	ret := false

	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if reduceTasksCompleted(c) {
		return true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.nMap = len(files)

	// create map tasks
	for i, file := range files {
		task := Task{
			taskID:    i,
			inputFile: file,
			state:     Idle,
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
			state:     Idle,
			workerID:  -1,
			startTime: 0,
		}
		c.reduceTasks = append(c.reduceTasks, &task)
	}

	// start the RPC server

	c.server()
	return &c
}
