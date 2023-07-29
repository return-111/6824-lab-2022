package mr

import (
	// "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const file_FREE = 0
const file_MAPPING = 1
const file_MAPED = 2

const reduce_FREE = 0
const reduce_BUSY = 1
const reduce_DONE = 2

const worker_free = 0
const worker_mapping = 1
const worker_reduceing = 2

type st struct {
	op, fid int
}

type Coordinator struct {
	// Your definitions here.
	files      []string
	filest     []int
	reducest   []int
	workerst   map[int]st
	filenum    int
	mapdone    int
	reducedone int
	nReduce    int
	lock       sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestHandler(args *RequestArgs, reply *RequestReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.workerst[args.Id].op == worker_mapping {
		fid := c.workerst[args.Id].fid
		assert(c.filest[fid] == file_MAPPING)
		c.filest[fid] = file_MAPED
		c.mapdone++
		c.workerst[args.Id] = st{worker_free, -1}
	} else if c.workerst[args.Id].op == worker_reduceing {
		fid := c.workerst[args.Id].fid
		assert(c.reducest[fid] == reduce_BUSY)
		c.reducest[fid] = reduce_DONE
		c.reducedone++
		c.workerst[args.Id] = st{worker_free, -1}
	}
	reply.Op = WAIT
	reply.Fid = -1
	reply.Name = ""
	reply.Nmap = c.filenum
	reply.Nreduce = c.nReduce
	if c.mapdone != c.filenum {
		for i, x := range c.filest {
			if x == file_FREE {
				reply.Op = MAP
				reply.Fid = i
				reply.Name = c.files[i]
				assert(c.workerst[args.Id].op == worker_free)
				c.workerst[args.Id] = st{worker_mapping, i}
				c.filest[i] = file_MAPPING
				go c.maptimer(args.Id, i) //timer
				break
			}
		}
		return nil
	} else if c.reducedone != c.nReduce {
		for i, x := range c.reducest {
			if x == reduce_FREE {
				reply.Op = REDUCE
				reply.Fid = i
				assert(c.workerst[args.Id].op == worker_free)
				c.workerst[args.Id] = st{worker_reduceing, i}
				c.reducest[i] = reduce_BUSY
				go c.reducetimer(args.Id, i) //timer
				break
			}
		}
		return nil
	} else {
		reply.Op = DONE
		return nil
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	ret := c.reducedone == c.nReduce
	c.lock.Unlock()
	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.filenum = len(files)
	c.filest = make([]int, c.filenum)
	c.mapdone = 0
	c.reducedone = 0
	c.reducest = make([]int, nReduce)
	c.nReduce = nReduce
	c.workerst = make(map[int]st)
	c.server()
	return &c
}

func (c *Coordinator) maptimer(workerid, fid int) {
	// fmt.Println("map sleep start")
	time.Sleep(time.Duration(10) * time.Second)
	// fmt.Println("map sleep finished")
	c.lock.Lock()
	pre := c.workerst[workerid]
	if pre.op == worker_mapping && pre.fid == fid {
		// fmt.Printf("%d map timeout!\n", pre.fid)
		assert(c.filest[pre.fid] == file_MAPPING)
		c.filest[pre.fid] = file_FREE
		c.workerst[workerid] = st{worker_free, -1}
	}
	c.lock.Unlock()
}

func (c *Coordinator) reducetimer(workerid, fid int) {
	time.Sleep(time.Duration(10) * time.Second)
	c.lock.Lock()
	pre := c.workerst[workerid]
	if pre.op == worker_reduceing && pre.fid == fid {
		assert(c.reducest[pre.fid] == reduce_BUSY)
		c.reducest[pre.fid] = reduce_FREE
		c.workerst[workerid] = st{worker_free, -1}
	}
	c.lock.Unlock()
}

func assert(cond bool) {
	if !cond {
		panic("assertion fault")
	}
}
