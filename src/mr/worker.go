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

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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
	taskCategory := TaskWait
	taskID := 0
	for {
		args := GetTaskArgs{
			LastCategory: taskCategory,
			LastID:       taskID,
		}
		reply := GetTaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if ok {
			taskCategory = reply.Category
			taskID = reply.ID
			if taskCategory == TaskMap {
				file, err := os.Open(reply.FileName)
				if err != nil {
					log.Fatalf("cannot open %v", reply.FileName)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", reply.FileName)
				}
				file.Close()
				kva := mapf(reply.FileName, string(content))
				files := make([]*os.File, reply.NReduce)
				encs := make([]*json.Encoder, reply.NReduce)
				for i := 0; i < reply.NReduce; i++ {
					files[i], err = os.Create(fmt.Sprintf("mr-%d-%d.json", reply.ID, i))
					// files[i], err = os.Create(fmt.Sprintf("mr-tmp/mr-%d-%d.json", reply.ID, i))
					if err != nil {
						log.Fatalf("cannot create %v", fmt.Sprintf("mr-tmp/mr-%d-%d.json", reply.ID, i))
					}
					encs[i] = json.NewEncoder(files[i])
				}
				for _, kv := range kva {
					err := encs[ihash(kv.Key)%reply.NReduce].Encode(&kv)
					if err != nil {
						log.Fatalf("cannot write json %v", kv)
					}
				}
				for _, f := range files {
					f.Close()
				}
			} else if taskCategory == TaskReduce {
				files, err := filepath.Glob(fmt.Sprintf("mr-*-%d.json", reply.ID))
				// files, err := filepath.Glob(fmt.Sprintf("mr-tmp/mr-*-%d.json", reply.ID))
				if err != nil {
					log.Fatalf("cannot find files: mr-tmp/mr-*-%d.json", reply.ID)
				}
				fl := []*os.File{}
				decs := []*json.Decoder{}
				kva := []KeyValue{}
				for _, file := range files {
					f, err := os.Open(file)
					if err != nil {
						log.Fatalf("cannot open %v", file)
					}
					fl = append(fl, f)
					decs = append(decs, json.NewDecoder(f))
				}
				for i := 0; i < len(decs); i++ {
					for {
						var kv KeyValue
						if err := decs[i].Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
				}
				sort.Sort(ByKey(kva))
				for _, f := range fl {
					f.Close()
				}
				oname := fmt.Sprintf("mr-out-%d", reply.ID)
				// oname := fmt.Sprintf("mr-tmp/mr-out-%d", reply.ID)
				ofile, _ := os.Create(oname)
				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
					i = j
				}
				ofile.Close()
			} else if taskCategory == TaskExit {
				break
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		} else {
			break
		}
	}
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
