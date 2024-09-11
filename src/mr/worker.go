package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// 实现对KeyValue数组的排序，实现了sort.Interface接口
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	mapFinish := false
	reduceFinish := false
	for !mapFinish || !reduceFinish {
		if !mapFinish {
			if ret, finish := DoMapTask(mapf); ret >= 0 {
				args := TaskFinishedArgs{TaskId: ret, Finished: true}
				reply := TaskFinishedReply{TaskId: ret, Finished: true}
				// 循环调用直至通知master服务器map任务完成
				for !call("Coordinator.MapFinish", &args, &reply) {
				}
			} else if finish {
				mapFinish = true
			}
		} else {
			if ret, finish := DoReduceTask(reducef); ret >= 0 {
				args := TaskFinishedArgs{TaskId: ret, Finished: true}
				reply := TaskFinishedReply{TaskId: ret, Finished: true}
				for !call("Coordinator.ReduceFinish", &args, &reply) {
				}
			} else if finish {
				reduceFinish = true
			}
		}
	}
}

// uncomment to send the Example RPC to the coordinator.
// CallExample()

func DoMapTask(mapf func(string, string) []KeyValue) (int, bool) {
	path, err := os.Getwd()
	if err != nil {
		// fmt.Printf("Getwd error !\n")
		return -1, false
	}
	args := AskMapArgs{WorkerMachine: Machine{Path: path}}
	reply := AskMapReply{}
	ok := call("Coordinator.AskMapTask", &args, &reply)
	if !ok {
		// fmt.Printf("call AskMapTask error !\n")
		return -1, false
	}
	if reply.TaskId == -1 {
		// fmt.Printf("All map task busy or finish !\n")
		return -1, reply.Finished
	}
	taskId := reply.TaskId
	filePath := reply.Path + "/" + reply.FileName
	nReduce := reply.NReduce
	// fmt.Printf("Get map task %d,filePath:%s,nReduce:%d\n", taskId, filePath, nReduce)
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("cannot open %v !\n", filePath)
		return -1, false
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v !\n", filePath)
		return -1, false
	}
	file.Close()
	// kva中间键值对数组
	kva := mapf(reply.FileName, string(content))
	// 中间文件
	middleFile := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		// 哈希算法取模，使键值对均匀分配到中间文件
		n := ihash(kv.Key) % nReduce
		middleFile[n] = append(middleFile[n], kv)
	}
	for i := 0; i < nReduce; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", taskId, i)
		file, _ := os.Create(fileName)
		enc := json.NewEncoder(file)
		for _, kv := range middleFile[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v !\n", fileName)
				return -1, false
			}
		}
	}
	file.Close()
	return taskId, false
}

func DoReduceTask(reducef func(string, []string) string) (int, bool) {
	path, err := os.Getwd()
	if err != nil {
		// fmt.Printf("Getwd error !\n")
		return -1, false
	}
	args := AskReduceArgs{WorkerMachine: Machine{Path: path}}
	reply := AskReduceReply{}
	ok := call("Coordinator.AskReduceTask", &args, &reply)
	if !ok {
		// fmt.Printf("call AskReduceTask error !\n")
		return -1, false
	}
	if reply.TaskId == -1 {
		// fmt.Printf("All reduce task busy or finish !\n")
		return -1, reply.Finished
	}
	middleFile := []KeyValue{}
	middleMachine := reply.MiddleMachine
	taskId := reply.TaskId
	for i, machine := range middleMachine {
		fileName := fmt.Sprintf("%s/mr-%d-%d", machine.Path, i, taskId)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v !\n", fileName)
			return -1, false
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			middleFile = append(middleFile, kv)
		}
	}
	sort.Sort(ByKey(middleFile))
	tmpFile, err := os.CreateTemp(path, "tmp")
	if err != nil {
		log.Fatalf("cannot create temp file !\n")
		return -1, false
	}
	// reduce操作,排序结束后，相同key的键值对会相邻
	// 提取所有相同key的键值对，传入reducef,并将结果写入临时文件，使主服务器无法观察部分完成的输出文件
	for i := 0; i < len(middleFile); {
		j := i + 1
		for j < len(middleFile) && middleFile[j].Key == middleFile[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, middleFile[k].Value)
		}
		output := reducef(middleFile[i].Key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", middleFile[i].Key, output)
		i = j
	}
	// 重命名输出文件
	newPath := fmt.Sprintf("%s/mr-out-%d", path, taskId)
	err = os.Rename(tmpFile.Name(), newPath)
	if err != nil {
		log.Fatalf("cannot rename %v !\n", newPath)
		return -1, false
	}
	tmpFile.Close()
	return taskId, false
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
// rpc函数调用方，向master发送rpc请求
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
