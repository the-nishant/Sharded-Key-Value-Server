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
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
	for {
		args := TaskArgs{}
		reply := TaskReply{}
		ok := call("Coordinator.Task", &args, &reply)
		if !ok || reply.Type == -1 {
			break
		}
		switch reply.Type {
		case 0:
			Map(&reply, mapf)
		case 1:
			Reduce(&reply, reducef)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func Map(reply *TaskReply, mapf func(string, string) []KeyValue) {
	filename := reply.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	n := reply.Other
	tmps := make([]*os.File, n)
	encs := make([]*json.Encoder, n)
	for i := 0; i < n; i++ {
		tmps[i], err = ioutil.TempFile("./", "temp-mr-")
		if err != nil {
			log.Fatalf("cannot make temporary file %v", i)
		}
		encs[i] = json.NewEncoder(tmps[i])
	}
	for j, kv := range kva {
		i := ihash(kv.Key) % n
		err := encs[i].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode key value pair %v", j)
		}
	}
	for i := 0; i < n; i++ {
		new_name := fmt.Sprintf("mr-%v-%v", reply.Index, i)
		err := os.Rename(tmps[i].Name(), new_name)
		if err != nil {
			log.Fatalf("cannot rename temporary file %v", i)
		}
		tmps[i].Close()
	}
	new_args := SuccessArgs{reply.Type, reply.Index}
	new_reply := SuccessReply{}
	call("Coordinator.Success", &new_args, &new_reply)
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Reduce(reply *TaskReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	n := reply.Other
	for i := 0; i < n; i++ {
		tname := fmt.Sprintf("mr-%v-%v", i, reply.Index)
		tmp, err := os.Open(tname)
		if err != nil {
			log.Fatalf("cannot open temporary file %v", tname)
		}
		dec := json.NewDecoder(tmp)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		tmp.Close()
	}
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%v", reply.Index)
	ofile, _ := os.Create(oname)
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	new_args := SuccessArgs{reply.Type, reply.Index}
	new_reply := SuccessReply{}
	call("Coordinator.Success", &new_args, &new_reply)
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
