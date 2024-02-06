package mapreduce

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type Coordinator struct {
	map_data         []MapData
	reduce_data      []ReduceData
	start_files_ch   chan string
	reduce_tasks_ch  chan int
	inter_files_chs  []chan safeString
	map_tasks_count  safeInt
	nReduce          int
	reduce_workerID  safeInt
	map_workerID     safeInt
	file_to_task     safeMapStringInt
	map_finish_mu    sync.Mutex
	ssh_clients      []*ssh.Client
	sftp_clients     []*sftp.Client
	data_root        string
	remote_data_root string
	remote_ids       []string
	remote_addresses []string
	remote_numbers   []int
	pattern          string
}

// TODO: put stuff in config files (addresses, grep pattern, data roots, remote ids)

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	// loop until a task is ready to assign or map reduce ends
	fmt.Printf("Inside AssignTask()...\n")
	for {
		select {
		case file := <-c.start_files_ch:
			if file == "" {
				continue
			}
			var map_task_id int
			if v, ok := c.file_to_task.get(file); ok { // already seen file (prev map task must have crashed)
				map_task_id = v
			} else { // new file
				map_task_id = c.map_tasks_count.get()
				c.map_tasks_count.set(map_task_id + 1)
			}
			c.map_data[map_task_id].state.set(InProgress)
			c.copyHostToRemote(c.data_root+file, c.remote_data_root+file, args.RemoteIndex)
			c.map_data[map_task_id].file.set(file)
			c.UpdateTimestamp(Map, map_task_id)

			reply.AssignedTaskType = Map
			reply.Filename = file
			reply.NReduce = c.nReduce
			reply.TaskID = map_task_id
			reply.WorkerID = c.map_workerID.get_and_increment()

			c.file_to_task.set(file, map_task_id)

			return nil
		default:
		}
		if c.MapPhaseDone() {
			select {
			case id := <-c.reduce_tasks_ch:

				fmt.Printf("\nAssigning reduce task %v for the first time NOW\n", id)

				c.reduce_data[id].state.set(InProgress)
				c.UpdateTimestamp(Reduce, id)

				reply.AssignedTaskType = Reduce
				reply.TaskID = id
				reply.WorkerID = c.reduce_workerID.get_and_increment()
				return nil
			default:
				time.Sleep(time.Millisecond * 10)
			}
		} else {
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (c *Coordinator) CheckWorkerStatus() {
	for {
		if c.Done() {
			return
		}

		for k := range c.map_data {
			task_data := &c.map_data[k]
			if task_data.state.get() == InProgress && time.Now().Sub(task_data.timestamp.get()).Seconds() >= 10 {
				// forget this worker and reassign task
				task_data.state.set(Unassigned)
				c.start_files_ch <- task_data.file.get()
			}
		}

		for k := range c.reduce_data {
			task_data := &c.reduce_data[k]
			if task_data.state.get() == InProgress && time.Now().Sub(task_data.timestamp.get()).Seconds() >= 10 {
				// forget this worker and reassign task
				task_data.state.set(Unassigned)
				c.reduce_tasks_ch <- k
				// task_data.files_processed.set(0)
			}
		}

		time.Sleep(1 * time.Second)
	}
}

func (c *Coordinator) MapFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	fmt.Printf("\nmap task #%v done\n\n", args.TaskID)
	c.map_finish_mu.Lock() // stop 2 maps from finishing simultaneously and both sending output to reducre
	defer c.map_finish_mu.Unlock()

	if c.map_data[args.TaskID].state.get() == Completed { // this is 2nd completion of task, don't need to worry about output anymore
		reply.Done = true
		return nil
	}

	for i := 0; i < c.nReduce; i += 1 {
		task_file := "mr-" + fmt.Sprint(args.TaskID) + "-" + fmt.Sprint(i)
		c.copyRemoteToHost(c.remote_data_root+task_file, c.data_root+task_file, args.RemoteIndex)
		if _, err := os.Stat(c.data_root + task_file); !errors.Is(err, os.ErrNotExist) { // if file exists
			c.reduce_data[i].task_files.append(task_file)
		}
	}

	c.map_data[args.TaskID].state.set(Completed)

	reply.Done = true
	return nil
}

func (c *Coordinator) ReduceFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	output_filename := "mr-out-" + fmt.Sprint(args.TaskID)
	c.copyRemoteToHost(c.remote_data_root+output_filename, c.data_root+output_filename, args.RemoteIndex)
	c.reduce_data[args.TaskID].state.set(Completed)
	reply.Done = true

	return nil
}

func (c *Coordinator) ReduceNextFile(args *ReduceFinishedPartialArgs, reply *ReduceFinishedPartialReply) error {
	files_processed := args.FilesProcessed
	for {
		c.UpdateTimestamp(Reduce, args.TaskID) // TODO: should this be for workerID?
		if files_processed < c.reduce_data[args.TaskID].task_files.len() {
			c.copyHostToRemote(c.data_root+c.reduce_data[args.TaskID].task_files.get(files_processed), c.remote_data_root+c.reduce_data[args.TaskID].task_files.get(files_processed), args.RemoteIndex)
			reply.NextFile = c.reduce_data[args.TaskID].task_files.get(files_processed)
			reply.Done = false
			return nil
		} else if c.MapPhaseDone() && files_processed >= c.reduce_data[args.TaskID].task_files.len() {
			reply.Done = true
			return nil
		}
		time.Sleep(time.Millisecond * 50) // wait for more files to (potentially) come from map tasks
	}
}

func (c *Coordinator) UpdateTimestamp(task_type TaskType, task_id int) {
	if task_type == Map {
		c.map_data[task_id].timestamp.set(time.Now())
	} else if task_type == Reduce {
		c.reduce_data[task_id].timestamp.set(time.Now())
	}
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) establishSSHConnection(remote_id string, address string) error {
	config := &ssh.ClientConfig{
		User: remote_id,
		Auth: []ssh.AuthMethod{
			ssh.Password(remote_id),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // TODO: this is insecure?
	}

	var err error
	ssh_client, err := ssh.Dial("tcp", address, config) // TODO: should this port be different from rpc port?
	if err != nil {
		log.Fatal("Failed to dial: ", err)
	}
	c.ssh_clients = append(c.ssh_clients, ssh_client)
	return err
}

func (c *Coordinator) addSFTPClient(ssh_client *ssh.Client) error {
	sftp_client, err := sftp.NewClient(ssh_client)
	if err != nil {
		fmt.Printf("Error with ssh or sftp: %v", err)
	}
	c.sftp_clients = append(c.sftp_clients, sftp_client)
	return err
}

func (c *Coordinator) copyHostToRemote(src string, dest string, remote_index int) error {
	src_file, err := os.Open(src)
	if err != nil {
		fmt.Printf("failed to open src (%s): %v\n", src, err)
	}

	client := c.sftp_clients[remote_index]
	dest_file, err := client.Create(dest)
	if err != nil {
		fmt.Printf("failed to create dest (%s): %v\n", dest, err)
	}

	_, err = io.Copy(dest_file, src_file)
	if err != nil {
		fmt.Printf("failed to copy src to dest: %v\n", err)
	}
	dest_file.Close()

	src_file.Close()
	return err
}

func (c *Coordinator) copyRemoteToHost(src string, dest string, remote_index int) error {
	dest_file, err := os.Create(dest)
	if err != nil {
		fmt.Printf("failed to create dest (%s): %v\n", dest, err)
	}

	client := c.sftp_clients[remote_index]
	src_file, err := client.Open(src)
	if err != nil {
		fmt.Printf("failed to open src (%s): %v\n", src, err)
	}

	_, err = io.Copy(dest_file, src_file)
	if err != nil {
		fmt.Printf("failed to copy src to dest: %v\n", err)
	}

	src_file.Close()

	dest_file.Close()
	return err
}

func (c *Coordinator) MapPhaseDone() bool {
	for i := range c.map_data {
		if c.map_data[i].state.get() != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) Done() bool {

	if !c.MapPhaseDone() {
		return false
	}

	for i := range c.reduce_data {
		if c.reduce_data[i].state.get() != Completed {
			return false
		}
	}

	fmt.Printf("\nMAP REDUCE DONE!\n\n")
	time.Sleep(1 * time.Second)

	for _, client := range c.ssh_clients {
		client.Close()
	}
	for _, client := range c.sftp_clients {
		client.Close()
	}

	return true
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	time.Sleep(1 * time.Second)

	c.data_root = "/tmp/mr-data/" // TODO: this data root prep should be done in mrcoordinator.go I think...
	os.MkdirAll(c.data_root, 0777)

	c.remote_data_root = "/tmp/mr/"

	c.nReduce = nReduce
	c.map_data = make([]MapData, len(files))
	c.reduce_data = make([]ReduceData, nReduce)
	c.file_to_task.make()

	c.remote_ids = make([]string, 2)
	c.remote_addresses = make([]string, 2)

	c.remote_ids[0] = "raspi1"
	c.remote_ids[1] = "raspi2"
	c.remote_addresses[0] = "192.168.2.200:22"
	c.remote_addresses[1] = "192.168.1.100:22"

	for i := range c.remote_ids {
		c.establishSSHConnection(c.remote_ids[i], c.remote_addresses[i])
		c.addSFTPClient(c.ssh_clients[i])
	}

	c.start_files_ch = make(chan string, len(files))
	// start tasks as unassigned
	for _, file := range files {
		c.start_files_ch <- file
	}
	c.reduce_tasks_ch = make(chan int, nReduce)
	for i := 0; i < nReduce; i += 1 {
		c.reduce_tasks_ch <- i
	}

	go c.CheckWorkerStatus()

	c.server()

	return &c
}
