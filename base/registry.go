package base

import (
	"sync"
	log "github.com/Sirupsen/logrus"
	"container/heap"
	"math"
	"sort"
)

type Registry struct {
	machines     map[int64]*Machine
	machineMutex sync.RWMutex

	jobs     map[int64]*Job
	jobMutex sync.RWMutex

	tasks     map[int64]*Task
	taskMutex sync.RWMutex

	events      *EventHeap
	eventsMutex sync.RWMutex
}

func NewRegistry(events *EventHeap) *Registry {
	return &Registry{
		machines: make(map[int64]*Machine),
		jobs:     make(map[int64]*Job),
		tasks:    make(map[int64]*Task),
		events:   events,
	}
}

func (r *Registry) GetJob(id int64) *Job {
	r.jobMutex.RLock()
	defer r.jobMutex.RUnlock()

	if job, ok := r.jobs[id]; ok {
		return job
	} else {
		return nil
	}
}

func (r *Registry) NextScheduleJob() *Job {
	r.jobMutex.Lock()
	defer r.jobMutex.Unlock()

	if len(r.jobs) == 0 {
		return nil
	}

	// TODO: sort more efficient
	v := make([]*Job, 0, len(r.jobs))
	for _, value := range r.jobs {
		if value.taskQueue.Len() > 0 {
			v = append(v, value)
		}
	}
	if len(v) == 0 {
		return nil
	}
	sort.Sort(JobShareSort(v))
	return v[0]
}

func (r *Registry) GetFirstTaskOfJob(job *Job) *Task {
	r.jobMutex.Lock()
	defer r.jobMutex.Unlock()

	if job, ok := r.jobs[job.JobID]; ok && job.taskQueue.Len() > 0 {
		return job.taskQueue.Peek()
	}
	return nil
}

func (r *Registry) TaskLenOfJob(job *Job) int {
	r.jobMutex.RLock()
	defer r.jobMutex.RUnlock()

	if job, ok := r.jobs[job.JobID]; ok {
		return job.taskQueue.Len()
	}
	return 0
}

func (r *Registry) RunTaskOfJob(job *Job) *Task {
	r.jobMutex.Lock()
	defer r.jobMutex.Unlock()

	task := job.taskQueue.PopTask()

	return task
}

func (r *Registry) JobLen() int {
	r.jobMutex.RLock()
	defer r.jobMutex.RUnlock()

	return len(r.jobs)
}

func (r *Registry) AddJob(job *Job) {
	r.jobMutex.Lock()
	defer r.jobMutex.Unlock()

	r.jobs[job.JobID] = job
}

func (r *Registry) UpdateJob(job *Job, task *Task, totalCpu, totalMem float64, add bool) {
	r.jobMutex.Lock()
	defer r.jobMutex.Unlock()

	if add {
		job.CpuUsed += task.CpuRequest
		job.MemUsed += task.MemoryRequest
	} else {
		job.CpuUsed -= task.CpuRequest
		job.MemUsed -= task.MemoryRequest
		job.TaskDone ++
	}
	job.Share = math.Max(job.CpuUsed/totalCpu, job.MemUsed/totalMem)
}

func (r *Registry) RemoveJob(job *Job) {
	r.jobMutex.Lock()
	defer r.jobMutex.Unlock()

	if _, ok := r.jobs[job.JobID]; ok {
		delete(r.jobs, job.JobID)
	}
}

func (r *Registry) AddTask(task *Task) {
	r.taskMutex.Lock()
	defer r.taskMutex.Unlock()

	if job, ok := r.jobs[task.JobID]; ok {
		job.taskQueue.PushTask(task)
		r.tasks[TaskID(task)] = task

	}
}

func (r *Registry) UpdateTask(task *Task) {
	r.taskMutex.Lock()
	defer r.taskMutex.Unlock()

	if _, ok := r.tasks[TaskID(task)]; ok {
		r.tasks[TaskID(task)] = task
	} else {
		log.Errorf("Task not found: job(%v) index(%v)", task.JobID, task.TaskIndex)
	}

}

func (r *Registry) RemoveTask(task *Task) {
	r.taskMutex.Lock()
	defer r.taskMutex.Unlock()

	if _, ok := r.tasks[TaskID(task)]; ok {
		delete(r.tasks, TaskID(task))
	} else {
		log.Errorf("Task not found: job(%v) index(%v)", task.JobID, task.TaskIndex)
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
