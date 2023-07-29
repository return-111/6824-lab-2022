package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		args := RequestArgs{os.Getpid()}
		reply := RequestReply{}
		ok := call("Coordinator.RequestHandler", &args, &reply)
		if !ok {
			return
		} else if reply.Op == DONE {
			return
		} else if reply.Op == WAIT {
			time.Sleep(time.Second)
			continue
		} else if reply.Op == MAP {
			nReduce := reply.Nreduce
			// fmt.Printf("%d map\n", reply.Fid)
			file, err := os.Open(reply.Name)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Name)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Name)
			}
			file.Close()
			kva := mapf(reply.Name, string(content))

			//Create temp file
			tempFile := make([]*os.File, nReduce)
			enc := make([]*json.Encoder, nReduce)

			for i := range tempFile {
				temp, err := ioutil.TempFile("./", "tmp-*")
				if err != nil {
					log.Fatalf("cannot create temp file")
				}
				tempFile[i] = temp
				enc[i] = json.NewEncoder(temp)
				defer temp.Close()
			}
			//Write to temp file
			for _, kv := range kva {
				buckid := ihash(kv.Key) % nReduce
				err := enc[buckid].Encode(&kv)
				if err != nil {
					log.Fatalf("failed to write file: %s kv: %v %v", tempFile[buckid].Name(), kv.Key, kv.Value)
				}
			}

			//Rename temp file
			for i := range tempFile {
				filename := fmt.Sprintf("mr-%d-%d", reply.Fid, i)
				err := os.Rename(tempFile[i].Name(), filename)
				if err != nil {
					log.Fatalf("cannot rename temp file")
				}
			}
			// filename := ""
			// fmt.Sprintf(filename, "mr-%d-%d")
			// err = os.Rename(tempFile.Name(), filename)
			// if err != nil {
			// 	log.Fatalf("cannot rename tempfile")
			// }
		} else if reply.Op == REDUCE {
			nMap := reply.Nmap

			//read intermediate
			intermediate := []KeyValue{}
			for i := 0; i < nMap; i++ {
				filename := fmt.Sprintf("mr-%d-%d", i, reply.Fid)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", reply.Fid)
			ofile, _ := os.Create(oname)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-*.
			//
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()
			// fmt.Printf("%d reduce\n", reply.Fid)
		} else {
			panic("111")
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
