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

var mu sync.Mutex

//var finishedMutex sync.Mutex
//
//var mapTaskMutex sync.Mutex
//
//var reduceTaskMutex sync.Mutex

type taskStatus int

type Phase int

type WorkerTaskStatus int

const (
	TaskPending taskStatus = iota
	TaskInPogress
	TaskCompleted
)

const (
	Success WorkerTaskStatus = iota
	TaskFailed
)

const (
	MapPhase Phase = iota
	ReducePhase
)

type Master struct {
	// Your definitions here.
	files               []string
	nMap              int
	nReduce           int
	mapTasks    []taskStatus
	reduceTasks []taskStatus
	curphase            Phase
	finished            bool
	doneMapNum          int
	doneReduceNum       int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	mu.Lock()
	if m.finished {
		mu.Unlock()
		reply.JobDone = true
		return nil
	}
	reply.JobDone = false
	if m.curphase == MapPhase {
		//mapTaskMutex.Lock()
		for i, status := range m.mapTasks {
			if status == TaskPending {
				reply.MapFile = m.files[i]
				reply.curphase = MapPhase
				reply.nMap = m.nMap
				reply.nReduce = m.nReduce
				reply.MapTaskIdx = i
				m.mapTasks[i] = TaskInPogress
				go func(mapTaskIdx int) {
					timer := time.NewTimer(time.Second * 10)
					<-timer.C
					mu.Lock()
					if m.mapTasks[mapTaskIdx] == TaskInPogress {
						m.mapTasks[mapTaskIdx] = TaskPending
					}
					mu.Unlock()
				}(i)
				break
			}
		}
		//mapTaskMutex.Unlock()
	} else if m.curphase == ReducePhase {
		//reduceTaskMutex.Lock()
		for i, status := range m.reduceTasks {
			if status == TaskPending {
				reply.curphase = ReducePhase
				reply.nMap = m.nMap
				reply.nReduce = m.nReduce
				reply.ReduceTaskIdx = i
				m.reduceTasks[i] = TaskInPogress
				go func(reduceTaskIdx int) {
					timer := time.NewTimer(time.Second * 10)
					<-timer.C
					mu.Lock()
					if m.reduceTasks[reduceTaskIdx] == TaskInPogress {
						m.reduceTasks[reduceTaskIdx] = TaskPending
					}
					mu.Unlock()
				}(i)
				break
			}
		}
		//reduceTaskMutex.Unlock()
	}
	mu.Unlock()

	return nil
}

func (m *Master) NotifyWorkerTaskStatus(args *NotifyWorkerTaskStatusArgs, reply *NotifyWorkerTaskStatusReply) error {
	mu.Lock()
	if args.WorkerPhase == MapPhase {
		//mapTaskMutex.Lock()
		if args.Status == Success {
			m.mapTasks[args.TaskIdx] = TaskCompleted
			m.doneMapNum++
			if m.doneMapNum == m.nMap {
				m.curphase = ReducePhase
			}
		} else if args.Status == TaskFailed {
			m.mapTasks[args.TaskIdx] = TaskPending
		} else {
			mu.Unlock()
		}

	} else if args.WorkerPhase == ReducePhase {

		if args.Status == Success {
			m.reduceTasks[args.TaskIdx] = TaskCompleted
			m.doneReduceNum++
			if m.doneReduceNum == m.nReduce {

				m.finished = true

			}
		} else if args.Status == TaskFailed {
			m.reduceTasks[args.TaskIdx] = TaskPending
		} else {
			mu.Unlock()
		}

	}
	mu.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()

	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls TaskCompleted() periodically to find out
// if the entire job has finished.
//
func (m *Master) TaskCompleted() bool {
	ret := false

	// Your code here.

	mu.Lock()
	ret = m.finished
	mu.Unlock()

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files: files,
		nMap: len(files),
		nReduce: nReduce,
		doneMapNum: 0,
		doneReduceNum: 0,
		finished: false,
		mapTasks: make([]taskStatus, len(files)),
		reduceTasks: make([]taskStatus, nReduce),
	}

	// Your code here.

	for i := 0; i < len(m.files); i++ {
		m.mapTasks[i] = TaskPending
	}

	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = TaskPending
	}

	m.server()
	return &m
}
