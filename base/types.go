package base

import (
	"strings"
	"errors"
	"strconv"
	"math"
)

func stringToInt64(str string, defaults int64) int64 {
	i, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return defaults
	}
	return i
}

func stringToFloat64(str string, defaults float64) float64 {
	f, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return defaults
	}
	return f
}

func stringToBool(str string, defaults bool) bool {
	if str == "0" {
		return false
	} else if str == "1" {
		return true
	} else {
		return defaults
	}
}

type Machine struct {
	MachineID  int64
	PlatformID string
	Cpus       float64
	Mem        float64
}

type Job struct {
	MissingInfo     int64
	JobID           int64        `csv:"job_id"`
	User            string
	SchedulingClass int64
	JobName         string
	LogicalJobName  string
	TaskNum         int64        `csv:"task_num"`
	SubmitTime      int64        `csv:"submit_time"`
	StartTime       int64        `csv:"start_time"`
	EndTime         int64        `csv:"end_time"`
	Duration        int64        `csv:"duration"`

	Share     float64
	index     int
	CpuUsed   float64
	MemUsed   float64
	taskDone  int64
	taskQueue chan *Task
}

func NewJob(job Job) *Job {
	return &Job{
		JobID:      job.JobID,
		SubmitTime: job.SubmitTime,
		StartTime:  job.StartTime,
		EndTime:    job.EndTime,
		TaskNum:    job.TaskNum,
		Duration:   job.Duration,

		taskQueue: make(chan *Task, 64),
	}

}

func (job *Job) UpdateStat(task *Task, totalCpu, totalMem float64, add bool) {
	if add {
		job.CpuUsed += task.CpuRequest
		job.MemUsed += task.MemoryRequest
	} else {
		job.CpuUsed -= task.CpuRequest
		job.MemUsed -= task.MemoryRequest
		job.taskDone ++
	}
	job.Share = math.Max(job.CpuUsed/totalCpu, job.MemUsed/totalMem)
}

func (job *Job) Done() bool {
	return job.taskDone == job.TaskNum
}

type TaskStatus int

const (
	TASK_STATUS_FINISHED TaskStatus = iota
	TASK_STATUS_RUNNING
	TASK_STATUS_STAGING
)

type Task struct {
	MissingInfo                  int64
	JobID                        int64 `csv:"job_id"`
	TaskIndex                    int64 `csv:"task_index"`
	MachineID                    int64
	User                         string
	SchedulingClass              int64
	Priority                     int64
	CpuRequest                   float64 `csv:"cpu_request"`
	MemoryRequest                float64 `csv:"memory_request"`
	DiskSpaceRequest             float64 `csv:"disk_space_request"`
	DifferentMachinesRestriction bool
	SubmitTime                   int64 `csv:"submit_time"`
	StartTime                    int64 `csv:"start_time"`
	EndTime                      int64 `csv:"end_time"`
	Duration                     int64 `csv:"duration"`

	Status TaskStatus
}

func NewTask(task Task) *Task {
	return &Task{
		JobID:            task.JobID,
		TaskIndex:        task.TaskIndex,
		CpuRequest:       task.CpuRequest,
		MemoryRequest:    task.MemoryRequest,
		DiskSpaceRequest: task.DiskSpaceRequest,
		SubmitTime:       task.SubmitTime,
		EndTime:          task.EndTime,
		StartTime:        task.StartTime,
		Duration:         task.Duration,
	}
}

func TaskID(task *Task) int64 {
	return task.JobID*1333 + task.TaskIndex
}

type MachineEventType int

const (
	MACHINE_ADD    MachineEventType = iota
	MACHINE_REMOVE
	MACHINE_UPDATE
)

type TaskEventType int

const (
	TASK_SUBMIT         TaskEventType = iota
	TASK_SCHEDULE
	TASK_EVICT
	TASK_FAIL
	TASK_FINISH
	TASK_KILL
	TASK_LOST
	TASK_UPDATE_PENDING
	TASK_UPDATE_RUNNING
)

type EventOrigin int

const (
	EVENT_MACHINE EventOrigin = iota
	EVENT_JOB
	EVENT_TASK
)

type Event struct {
	Time             int64
	Machine          *Machine
	Job              *Job
	Task             *Task
	EventOrigin      EventOrigin
	TaskEventType    TaskEventType
	MachineEventType MachineEventType
}

type EventChans map[EventOrigin]chan *Event

type EventSlice [] *Event

func (e EventSlice) Len() int {
	return len(e)
}

func (e EventSlice) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

func (e EventSlice) Less(i, j int) bool {
	return e[j].Time > e[i].Time
}

func ParseMachineEvent(line string) (*Event, error) {
	splits := strings.Split(line, ",")
	if len(splits) != 6 {
		return nil, errors.New("machine event lost some fields")
	}

	machine := &Machine{
		MachineID:  stringToInt64(splits[1], 0),
		PlatformID: splits[3],
		Cpus:       stringToFloat64(splits[4], 0.0),
		Mem:        stringToFloat64(splits[5], 0.0),
	}

	return &Event{
		Time:             stringToInt64(splits[0], 0),
		MachineEventType: MachineEventType(stringToInt64(splits[2], 0)),
		Machine:          machine,
		EventOrigin:      EVENT_MACHINE,
	}, nil
}

func ParseTaskEvent(line string) (*Event, error) {
	splits := strings.Split(line, ",")
	if len(splits) != 13 {
		return nil, errors.New("task event lost some fields")
	}

	task := &Task{
		JobID:                        stringToInt64(splits[2], 0),
		TaskIndex:                    stringToInt64(splits[3], 0),
		MachineID:                    stringToInt64(splits[4], 0),
		User:                         splits[6],
		SchedulingClass:              stringToInt64(splits[7], 0),
		Priority:                     stringToInt64(splits[8], 0),
		CpuRequest:                   stringToFloat64(splits[9], 0.0),
		MemoryRequest:                stringToFloat64(splits[10], 0.0),
		DiskSpaceRequest:             stringToFloat64(splits[11], 0.0),
		DifferentMachinesRestriction: stringToBool(splits[12], false),
	}

	return &Event{
		Time:          stringToInt64(splits[0], 0),
		TaskEventType: TaskEventType(stringToInt64(splits[5], 0)),
		Task:          task,
		EventOrigin:   EVENT_TASK,
	}, nil
}

func ParseJobEvent(line string) (*Event, error) {
	splits := strings.Split(line, ",")
	if len(splits) != 8 {
		return nil, errors.New("job event lost some fields")
	}

	job := &Job{
		JobID:           stringToInt64(splits[2], 0),
		User:            splits[4],
		SchedulingClass: stringToInt64(splits[5], 0),
		JobName:         splits[6],
		LogicalJobName:  splits[7],
	}

	return &Event{
		Time:          stringToInt64(splits[0], 0),
		TaskEventType: TaskEventType(stringToInt64(splits[3], 0)),
		Job:           job,
		EventOrigin:   EVENT_JOB,
	}, nil
}
