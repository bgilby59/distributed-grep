package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
)

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var data_root string = "/tmp/mr/"
var remote_index int = 0 // TODO: better way of assigning this

func reduceDistinctKeys(kvs []KeyValue, reducef func(string, []string) string) map[string]string {
	res := make(map[string]string)
	sort.Sort(ByKey(kvs))
	for i := 0; i < len(kvs); i += 1 {
		key := kvs[i].Key
		var values []string
		values = append(values, kvs[i].Value)
		for j := i + 1; j < len(kvs); j += 1 {
			if kvs[j].Key == key {
				values = append(values, kvs[j].Value)
			} else {
				i = j - 1
				break
			}
			if j == len(kvs)-1 {
				i = j
			}
		}
		res[key] = reducef(key, values)
	}
	return res
}

func readReduceInput(files []string) []KeyValue { // map string (key) to slice of strings (values)
	var kva []KeyValue
	for _, filename := range files {
		fmt.Printf("Reading: %s\n", filename)
		file_to_read, err := os.OpenFile(filename, os.O_RDWR, 0666)
		if err != nil {
			fmt.Printf("Error opening file %s\n", filename)
		}
		dec := json.NewDecoder(file_to_read)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil { // EOF?
				break
			}
			kva = append(kva, kv)
		}

		err = file_to_read.Close()
		if err != nil {
			fmt.Printf("Error closing file %s\n", filename)
		}
	}

	return kva
}

func readMapInput(filename string) (string, error) {
	contents, err := os.ReadFile(filename)

	return string(contents), err
}

func writeMapOutput(output []KeyValue, nReduce int, taskID int, workerID int) bool {
	for i := 0; i < nReduce; i += 1 {
		tmp_inter_output_file := "mr-worker" + fmt.Sprint(workerID) + "-" + fmt.Sprint(i)
		inter_output_file := data_root + "mr-" + fmt.Sprint(taskID) + "-" + fmt.Sprint(i)
		var file_to_write *os.File
		var err error
		file_to_write, err = os.OpenFile(tmp_inter_output_file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			fmt.Println("error opening intermediate file")
		}
		for _, kv := range output {
			if ihash(kv.Key)%nReduce != i {
				continue
			} // separate writes by file so that file is not constantly opened and closed
			enc := json.NewEncoder(file_to_write)
			err = enc.Encode(&kv)
			if err != nil {
				fmt.Printf("error writing to intermediate file: %v\n", err)
				return false
			}
		}
		err = file_to_write.Close()
		if err != nil {
			fmt.Println("error closing intermediate file")
		}
		err = os.Rename(tmp_inter_output_file, inter_output_file)
		if err != nil {
			fmt.Println("error renaming intermediate file")
		}
	}
	return true
}

func writeReduceOutput(filename string, data string) error {
	file_to_write, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("error opening tmp output file")
		return err
	}
	_, err = file_to_write.WriteString(data)
	if err != nil {
		fmt.Println("error writing to tmp output file")
		return err
	}
	err = file_to_write.Close()
	return err
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	os.MkdirAll(data_root, 0777)

	for {
		fmt.Printf("Calling AssignTask on coordinator...\n")
		task_info, ok := CallAssignTask()
		if !ok {
			continue
		}

		if task_info.AssignedTaskType == Map {
			contents, err := readMapInput(data_root + task_info.Filename)
			if err != nil {
				fmt.Println("error reading map input")
			}
			map_res := mapf(task_info.Filename, contents)
			writeMapOutput(map_res, task_info.NReduce, task_info.TaskID, task_info.WorkerID)
			CallMapFinished(task_info.TaskID)
		} else if task_info.AssignedTaskType == Reduce {
			var files []string
			tmp_output_file := "mr-out-" + fmt.Sprint(task_info.TaskID) + "-worker" + fmt.Sprint(task_info.WorkerID)
			_, err := os.Create(tmp_output_file)
			if err != nil {
				fmt.Printf("Error creating file %s: %v", tmp_output_file, err)
			}
			for {
				reply, _ := CallReduceNextFile(task_info.TaskID, len(files))
				if reply.Done {
					break
				} else {
					files = append(files, data_root+reply.NextFile)
				}
			}
			reduce_kv := readReduceInput(files)
			reduce_res := reduceDistinctKeys(reduce_kv, reducef)
			for k, v := range reduce_res {
				err := writeReduceOutput(tmp_output_file, fmt.Sprintf("%v %v\n", k, v))
				if err != nil {
					fmt.Println("error writing to tmp output file")
				}
			}
			err = os.Rename(tmp_output_file, data_root+"mr-out-"+fmt.Sprint(task_info.TaskID)) // is this atomic??
			if err != nil {
				fmt.Printf("error renaming to output file: %v\n", err)
			} else {
				fmt.Printf("Success! Renamed %s to %s\n", tmp_output_file, "mr-out-"+fmt.Sprint(task_info.TaskID))
			}
			CallReduceFinished(task_info.TaskID)
		}
	}
}

func CallMapFinished(task_id int) (TaskFinishedReply, bool) {
	args := TaskFinishedArgs{TaskID: task_id, RemoteIndex: remote_index}

	reply := TaskFinishedReply{}

	ok := call("Coordinator.MapFinished", &args, &reply)

	return reply, ok
}

func CallReduceFinished(task_id int) (TaskFinishedReply, bool) {
	args := TaskFinishedArgs{TaskID: task_id, RemoteIndex: remote_index}

	reply := TaskFinishedReply{}

	ok := call("Coordinator.ReduceFinished", &args, &reply)

	return reply, ok
}

func CallReduceNextFile(taskID int, files_processed int) (ReduceFinishedPartialReply, bool) {
	args := ReduceFinishedPartialArgs{TaskID: taskID, FilesProcessed: files_processed}

	reply := ReduceFinishedPartialReply{}

	ok := call("Coordinator.ReduceNextFile", &args, &reply)

	return reply, ok
}

func CallAssignTask() (AssignTaskReply, bool) {
	args := AssignTaskArgs{RemoteIndex: remote_index}

	reply := AssignTaskReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)

	return reply, ok
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "192.168.2.20"+":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}
