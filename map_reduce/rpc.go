package mapreduce

type TaskType int

const (
	Map    TaskType = iota
	Reduce TaskType = iota
)

type AssignTaskArgs struct {
	ID          int
	RemoteIndex int
}

type AssignTaskReply struct {
	AssignedTaskType TaskType
	Filename         string
	TaskID           int
	NReduce          int
	WorkerID         int
}

type TaskFinishedArgs struct {
	TaskID      int
	WorkerID    int
	RemoteIndex int
}

type TaskFinishedReply struct {
	Done bool
}

type ReduceFinishedPartialArgs struct {
	TaskID         int
	FilesProcessed int
	RemoteIndex    int
}

type ReduceFinishedPartialReply struct {
	NextFile string
	Done     bool
}
