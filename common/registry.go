package common

import (
	"sync"
	log "github.com/Sirupsen/logrus"
	"math"
	"container/heap"
)

type Registry struct {
	machines     map[int64]*Machine
	machineMutex sync.RWMutex

	jobs             *PriorityQueue
	TotalWaitingTime int64 // start_time - submit_time
	TotalRunningTime int64 // end_time - submit_time

	tasks          map[int64]*Task
	runningService map[int64]*Task
	taskMutex      sync.RWMutex

	events      *EventHeap
	eventsMutex sync.RWMutex

	totalIndex    float64
	countIndex    int
	lastIndexTime int64

	MaxIndex float64
	MinIndex float64
}

func NewRegistry(events *EventHeap) *Registry {
	jobs := new(PriorityQueue)
	jobs.Init(10000000)

	return &Registry{
		machines:       make(map[int64]*Machine),
		jobs:           jobs,
		tasks:          make(map[int64]*Task),
		runningService: make(map[int64]*Task),
		events:         events,

		TotalRunningTime: 0,
		TotalWaitingTime: 0,

		MaxIndex:      0,
		MinIndex:      1,
		lastIndexTime: 0,
		totalIndex:    0.0,
	}
}

func (r *Registry) CountJainsFairIndex(time int64) float64 {
	jobs := r.jobs.GetItems()
	count := 0
	totalCpu := 0.0
	totalMem := 0.0
	totalSquareCpu := 0.0
	totalSquareMem := 0.0

	for _, job := range jobs {

		if job.CpuUsed > 0 || job.MemUsed > 0 {
			//log.Info(job.JobID, job.CpuUsed, job.MemUsed)
			count++

			totalCpu += job.CpuUsed
			totalMem += job.MemUsed
			totalSquareCpu += job.CpuUsed * job.CpuUsed
			totalSquareMem += job.MemUsed * job.MemUsed
		}
	}

	var index float64
	if ((totalSquareCpu+totalSquareMem) > 0 && count != 0) {
		index = (totalCpu*totalCpu + totalMem*totalMem) / (totalSquareCpu + totalSquareMem) / float64(count)
	} else {
		index = 1
	}

	log.Debug(index)
	r.totalIndex += float64(time-r.lastIndexTime) * index
	r.lastIndexTime = time
	return index
}

func (r *Registry) GetJainsFairIndex(duration int64) float64 {
	if duration == 0 {
		return 0
	}
	return r.totalIndex / float64(duration)
}

func (r *Registry) GetJob(id int64) *Job {
	return r.jobs.GetItem(id)
}

func (r *Registry) NextScheduleJob() *Job {
	if r.jobs.Len() == 0 {
		return nil
	}

	job := r.jobs.MinItem()

	return job
}

func (r *Registry) TaskLenOfJob(job *Job) int {
	j := r.jobs.GetItem(job.JobID)
	if j != nil {
		return len(j.taskQueue)
	}
	return 0
}

func (r *Registry) JobLen() int {
	return r.jobs.Len()
}

func (r *Registry) AddJob(job *Job) {
	r.jobs.PushItem(job.JobID, NewJob(*job))
}

func (r *Registry) UpdateJob(job *Job, task *Task, add bool) {
	if add {
		job.CpuUsed += task.CpuRequest
		job.MemUsed += task.MemoryRequest
	} else {
		job.CpuUsed -= task.CpuRequest
		job.MemUsed -= task.MemoryRequest
		job.TaskDone ++
	}

	totalCpu, totalMem := r.TotalResources()
	job.Share = math.Max(job.CpuUsed/totalCpu, job.MemUsed/totalMem)
	r.jobs.UpdateItem(job.JobID, job)
}

func (r *Registry) WaitingTasks(job *Job) map[int64]*Task {
	result := make(map[int64]*Task)
	for _, task := range job.taskQueue {
		if task.Status == TASK_STATUS_STAGING {
			result[task.TaskIndex] = task
		}
	}
	return result
}

func (r *Registry) RemoveJob(job *Job, time int64) {
	r.jobs.RemoveItem(job.JobID)

	job.EndTime = time
	r.TotalWaitingTime += job.StartTime - job.SubmitTime
	r.TotalRunningTime += job.EndTime - job.SubmitTime
}

func (r *Registry) AddTask(task *Task) {
	job := r.jobs.GetItem(task.JobID)
	if job != nil {
		if t, ok := job.taskQueue[task.TaskIndex]; ok {
			log.Debugf("Kill old task and submit new task for task(%v) of job(%v)", t.TaskIndex, t.JobID)

		}
		job.taskQueue[task.TaskIndex] = task
		r.tasks[TaskID(task)] = task
		r.jobs.UpdateItem(job.JobID, job)
	}
}

func (r *Registry) TaskLen() int {
	r.taskMutex.RLock()
	defer r.taskMutex.RUnlock()

	return len(r.tasks)
}

func (r *Registry) UpdateTask(task *Task) {
	r.taskMutex.Lock()
	defer r.taskMutex.Unlock()

	if _, ok := r.tasks[TaskID(task)]; ok {
		r.tasks[TaskID(task)] = task
	} else {
		log.Errorf("Task not found when update task: job(%v) index(%v)", task.JobID, task.TaskIndex)
	}

}

func (r *Registry) RemoveTask(task *Task) {
	r.taskMutex.Lock()
	defer r.taskMutex.Unlock()

	if _, ok := r.tasks[TaskID(task)]; ok {
		delete(r.tasks, TaskID(task))
	} else {
		log.Errorf("Task not found when remove task: job(%v) index(%v)", task.JobID, task.TaskIndex)
	}
}

func (r *Registry) ArchiveTask() map[TaskStatus][]*Task {
	r.taskMutex.Lock()
	defer r.taskMutex.Unlock()
	archive := make(map[TaskStatus][]*Task)
	for _, task := range r.tasks {
		archive[task.Status] = append(archive[task.Status], task)
	}
	return archive
}

func (r *Registry) FilterTask(filterFunc func(*Task) bool) []*Task {
	r.taskMutex.RLock()
	defer r.taskMutex.RUnlock()

	result := []*Task{}
	for _, task := range r.tasks {
		if filterFunc(task) {
			result = append(result, task)
		}
	}
	return result
}

func (r *Registry) AddMachine(machine *Machine) {
	r.machineMutex.Lock()
	defer r.machineMutex.Unlock()

	r.machines[machine.MachineID] = machine
}

func (r *Registry) UpdateMachine(machine *Machine) {
	r.machineMutex.Lock()
	defer r.machineMutex.Unlock()

	if m, ok := r.machines[machine.MachineID]; ok {
		m.Cpus = machine.Cpus
		m.Mem = machine.Mem
	}
}

func (r *Registry) RemoveMachine(machine *Machine) {
	r.machineMutex.Lock()
	defer r.machineMutex.Unlock()

	if _, ok := r.machines[machine.MachineID]; ok {
		delete(r.machines, machine.MachineID)
	}
}

func (r *Registry) RunOnMachine(task *Task) {
	r.machineMutex.Lock()
	defer r.machineMutex.Unlock()

	if m, ok := r.machines[task.MachineID]; ok {
		m.UsedCpus += task.CpuRequest
		m.UsedMem += task.MemoryRequest
	}
}

func (r *Registry) CompleteOnMachine(task *Task) {
	r.machineMutex.Lock()
	defer r.machineMutex.Unlock()

	if m, ok := r.machines[task.MachineID]; ok {
		m.UsedCpus -= task.CpuRequest
		m.UsedMem -= task.MemoryRequest
	}
}

func (r *Registry) TotalResources() (float64, float64) {
	r.machineMutex.RLock()
	defer r.machineMutex.RUnlock()

	totalCpus := 0.0
	totalMem := 0.0
	for _, machine := range r.machines {
		totalCpus += machine.Cpus
		totalMem += machine.Mem
	}

	return totalCpus, totalMem
}

func (r *Registry) TaskCanRunOnMachine(task *Task) (bool, *Machine) {
	r.machineMutex.RLock()
	defer r.machineMutex.RUnlock()

	for _, machine := range r.machines {
		if (machine.Cpus-machine.UsedCpus) > task.CpuRequest && (machine.Mem-machine.UsedMem) > task.MemoryRequest {
			return true, machine
		}
	}

	return false, nil
}

func (r *Registry) ResourceOffers() ([]*Machine) {
	r.machineMutex.RLock()
	defer r.machineMutex.RUnlock()

	var offers []*Machine
	for _, machine := range r.machines {
		if machine.Cpus > machine.UsedCpus && machine.Mem > machine.UsedMem {
			offers = append(offers, machine)
		}
	}

	return offers
}

func (r *Registry) LenEvent() int {
	return len(*r.events)
}

func (r *Registry) PushEvent(event *Event) {
	r.eventsMutex.Lock()
	defer r.eventsMutex.Unlock()

	heap.Push(r.events, event)
}

func (r *Registry) PopEvent() *Event {
	r.eventsMutex.Lock()
	defer r.eventsMutex.Unlock()

	return heap.Pop(r.events).(*Event)
}

func (r *Registry) TopEvent() *Event {
	r.eventsMutex.RLock()
	defer r.eventsMutex.RUnlock()

	l := *r.events
	return l[0]
}

func (r *Registry) AllDone() bool {
	for _, task := range r.tasks {
		if task.Status == TASK_STATUS_STAGING {
			return false
		}
	}
	return true
}
