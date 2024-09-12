package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string // 每个单词作为键
	Value string // 频率作为值
}

// for sorting by key.
type ByKey []KeyValue

// 实现sort接口,shuffle，对key排序
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
// ihash将键映射到Reduce任务中（中间文件）
// reduce 任务读的是mr-0-y、mr-1-y、mr-2-y、...的列表中间文件
// 将文件读入内存，然后统一按照 key 排序和 shuffle
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
// loadPlugin后传入mapf和reducef
// 最终输出键值对并输入到intermediate：func mapf(filename string, contents string) []KeyValue
// func reducef(key string, values []string) string
// mrsequential是示例，由mrworker调用Worker并传入mapf和reducef
// worker通过rpc调用coordinator的方法
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	mapFinish:=false
	reduceFinish:=false
	for !mapFinish||!reduceFinish{
		// worker需要阻塞，只有最后一个 map 任务完成，reduce 任务才能启动
		// 也可以写成coordinator发送消息是否开始reduce
		if!mapFinish{
			if ret,ok:=DoMapTask(mapf);ret>=0{
				args:=TaskFinishArgs{ret}
				reply:=TaskFinishReply{}
				// 非阻塞式，轮询，确认map是否全部完成
				for !call("Coordinator.MapFinish",&args,&reply){}
			}else if ok{
				// 全部完成
				mapFinish=true
			}
		}else{
			// 开始reduce
			if ret,ok:=DoReduceTask(reducef);ret>=0{
				args:=TaskFinishArgs{ret}
				reply:=TaskFinishReply{}
				// 轮询reduce任务是否完成
				for !call("Coordinator.ReduceFinish",&args,&reply){}
			}else if ok{
				reduceFinish=true
			}
		}
	}
	// 全部完成
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// worker有两种状态，执行reduce任务或map任务
// 返回taskId和执行状态, FAILED表示无id
func DoMapTask(mapf func(string, string) []KeyValue) (int, bool) {
	path, err := os.Getwd()
	if err != nil {
		return FAILED, false
	}
	// worker地址
	args := AskMapArgs{Machine{path}}
	reply := AskMapReply{} //coordinator填充
	// 调用map请求
	ok := call("Coordinator.AskMapTask", &args, &reply)
	if !ok {
		return FAILED, false
	}
	if reply.TaskId == ALL_FINISH || reply.TaskId == BUSY {
		return FAILED, reply.AllFinish
	}
	// 执行map任务
	taskId := reply.TaskId
	nReduce := reply.NReduce
	filePath := reply.Path + "/" + reply.FileName
	// 借鉴mrsequential.go的代码
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("cannot open %v file", filePath)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v file", filePath)
	}
	file.Close()
	// 从input file提取的键值对keyValue
	kva := mapf(reply.FileName, string(content))
	// 将keyValue按哈希值分区，使相同的键值对在同一分区
	intermediate := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		// ihash将键均匀分布到reduce任务
		i := ihash(kv.Key) % nReduce
		intermediate[i] = append(intermediate[i], kv)
	}
	for i := 0; i < nReduce; i++ {
		// 中间文件格式
		fileName := fmt.Sprintf("mr-%d-%d", taskId, i)
		file, err := os.Create(fileName)
		if err != nil {
			log.Fatalf("cannot create %v file", fileName)
		}
		// 使用json将键值对输入到intermediate
		enc := json.NewEncoder(file)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v file", fileName)
				return FAILED, false
			}
		}
		file.Close()
	}
	// map任务完成，同时获得intermediate
	return taskId, false
}

func DoReduceTask(reducef func(string, []string) string) (int, bool) {
	// 获取工作目录路径
	path, err := os.Getwd()
	if err != nil {
		return FAILED, false
	}
	args := AskReduceArgs{Machine{path}}
	reply := AskReduceReply{}
	// 调用reduce请求
	ok := call("Coordinator.AskReduceTask", &args, &reply)
	if !ok {
		return FAILED, false
	}
	if reply.TaskId == ALL_FINISH || reply.TaskId == BUSY {
		return FAILED, reply.AllFinish
	}
	// 读取intermediate files
	intermediate := []KeyValue{}
	intermediateWorkers := reply.IntermediateWorkers
	taskId := reply.TaskId
	for i, worker := range intermediateWorkers {
		// 读取的中间文件
		fileName := fmt.Sprintf("%s/mr-%d-%d", worker.Path, i, taskId)
		file, err := os.Open(fileName)
		if err != nil {
			return FAILED, false
		}
		dec := json.NewDecoder(file)
		// 课程提示
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	// 执行排序，将相同的key放在一起
	sort.Sort(ByKey(intermediate))
	// 创建临时文件，输出reduce的结果
	tmpFile,err:=ioutil.TempFile(path,"tmp")
	if err!=nil{
		return FAILED,false
	}
	// 外部排序,将相同的键值对放到一起,然后reduce生成reduce output
	for i:=0;i<len(intermediate);{
		j:=i+1
		for j<len(intermediate)&&intermediate[j].Key==intermediate[i].Key{
			j++
		}
		values:=[]string{}
		for k:=i;k<j;k++{
			values=append(values,intermediate[k].Value)
		}
		output:=reducef(intermediate[i].Key,values)
		// reduce output file输出格式
		fmt.Fprintf(tmpFile,"%v %v\n",intermediate[i].Key,output)
		i=j
	}
	// 最终output file的格式
	newPath:=fmt.Sprintf("%s/mr-out-%d",path,taskId)
	// 修正tmpFile的名字，处理crash
	// 对于map 任务，此时文件还没打开和生成，无需处理
	// 对于 reduce 任务，此时结果文件已经打开，所以需要新建temp, 避免worker崩溃后在磁盘生成错误结果
	err=os.Rename(tmpFile.Name(),newPath)
	if err!=nil{
		return FAILED,false
	}
	tmpFile.Close()
	// reduce任务完成
	return taskId, false
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
// worker调用coordinator的方法，args由worker填充，reply由coordinator填充
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// worker调用rpc与coordinator通信
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	// 创建socket
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	// 远程调用方法
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
