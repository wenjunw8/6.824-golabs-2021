package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

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

func processMapTask(task *TaskDesc, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(task.InputFile)
	if err != nil {
		log.Fatalf("cannot read %v", task)
	}

	kva := mapf(task.InputFile, string(content))

	// write to intermediate files
	nReduce := task.NumReduce
	intermediateFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
		intermediateFiles[i], _ = os.Create(intermediateFileName)
		encoders[i] = json.NewEncoder(intermediateFiles[i])
	}
	for _, kv := range kva {
		reduceTaskNum := ihash(kv.Key) % nReduce
		encoders[reduceTaskNum].Encode(&kv)
	}
	for i := 0; i < nReduce; i++ {
		intermediateFiles[i].Close()
	}
}

func processReduceTask(task *TaskDesc, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	nMap := task.NumMap
	for i := 0; i < nMap; i++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", i, task.TaskID)
		file, err := os.Open(intermediateFileName)
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFileName)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	kvs := make(map[string][]string)
	for _, kv := range intermediate {
		kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
	}
	outputFileName := fmt.Sprintf("mr-out-%d", task.TaskID)
	outputFile, _ := os.Create(outputFileName)
	for key, values := range kvs {
		output := reducef(key, values)
		fmt.Fprintf(outputFile, "%v %v\n", key, output)
	}
	outputFile.Close()
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task := CallAssignTask()
		switch task.Type {
		case MapTask:
			processMapTask(task, mapf)
			CallReportTaskCompletion(MapTask, task.TaskID)
		case ReduceTask:
			processReduceTask(task, reducef)
			CallReportTaskCompletion(1, task.TaskID)
		case WaitTask:
			time.Sleep(10 * time.Millisecond)
			continue
		case ExitTask:
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func CallAssignTask() *TaskDesc {
	args := myId()
	reply := TaskDesc{}

	if !call("Coordinator.AssignTask", &args, &reply) {
		os.Exit(0)
	}

	return &reply
}

func myId() int {
	return os.Getpid()
}

func CallReportTaskCompletion(taskType TaskType, taskID int) bool {
	args := TaskDesc{
		Type:   taskType,
		TaskID: taskID,
	}
	reply := 0

	if !call("Coordinator.ReportTaskCompletion", &args, &reply) {
		os.Exit(0)
	}
	return true
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
