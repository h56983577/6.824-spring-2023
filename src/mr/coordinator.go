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
	mapTasks     []MapTask
	reduceTasks  []ReduceTask
	isMapDone    bool
	isReduceDone bool
	exitTime     time.Time
	nReduce      int
	mutex        sync.Mutex
}

type Status int

const (
	Idle Status = iota
	InProgress
	Completed
)

type MapTask struct {
	FileName   string
	ExpiredAt  time.Time
	TaskStatus Status
}

type ReduceTask struct {
	ExpiredAt  time.Time
	TaskStatus Status
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
	c.mutex.Lock()
	switch args.LastCategory {
	case TaskMap:
		c.mapTasks[args.LastID].TaskStatus = Completed
	case TaskReduce:
		c.reduceTasks[args.LastID].TaskStatus = Completed
	}
	if !c.isMapDone {
		reply.Category = TaskWait
		isAllDone := true
		for i := 0; i < len(c.mapTasks); i++ {
			var task = c.mapTasks[i]
			if task.TaskStatus == Idle || (task.TaskStatus == InProgress && task.ExpiredAt.Before(time.Now())) {
				c.mapTasks[i].TaskStatus = InProgress
				c.mapTasks[i].ExpiredAt = time.Now().Add(10 * time.Second)
				reply.Category = TaskMap
				reply.ID = i
				reply.FileName = task.FileName
				reply.NReduce = c.nReduce
				break
			}
		}
		for i := 0; i < len(c.mapTasks); i++ {
			if c.mapTasks[i].TaskStatus != Completed {
				isAllDone = false
				break
			}
		}
		c.isMapDone = isAllDone
	} else if !c.isReduceDone {
		reply.Category = TaskWait
		isAllDone := true
		for i := 0; i < len(c.reduceTasks); i++ {
			var task = c.reduceTasks[i]
			if task.TaskStatus == Idle || (task.TaskStatus == InProgress && task.ExpiredAt.Before(time.Now())) {
				c.reduceTasks[i].TaskStatus = InProgress
				c.reduceTasks[i].ExpiredAt = time.Now().Add(10 * time.Second)
				reply.Category = TaskReduce
				reply.ID = i
				reply.FileName = ""
				reply.NReduce = c.nReduce
				break
			}
		}
		for i := 0; i < len(c.reduceTasks); i++ {
			if c.reduceTasks[i].TaskStatus != Completed {
				isAllDone = false
				break
			}
		}
		c.isReduceDone = isAllDone
		if isAllDone {
			c.exitTime = time.Now().Add(10 * time.Second)
		}
	} else {
		reply.Category = TaskExit
	}
	c.mutex.Unlock()
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
	if c.isReduceDone && c.exitTime.Before(time.Now()) {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// _, err := os.Stat("mr-tmp")
	// if !os.IsNotExist(err) {
	// 	err = os.RemoveAll("mr-tmp")
	// 	if err != nil {
	// 		log.Fatalf("cannot clear %v", "mr-tmp")
	// 	}
	// }
	// err = os.MkdirAll("mr-tmp", 0755)
	// if err != nil {
	// 	log.Fatalf("cannot mkdir %v", "mr-tmp")
	// }
	c.mapTasks = make([]MapTask, len(files))
	c.reduceTasks = make([]ReduceTask, nReduce)
	c.nReduce = nReduce
	for i := 0; i < len(files); i++ {
		c.mapTasks[i] = MapTask{
			FileName:   files[i],
			ExpiredAt:  time.Now(),
			TaskStatus: Idle,
		}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{
			ExpiredAt:  time.Now(),
			TaskStatus: Idle,
		}
	}

	c.server()
	return &c
}
