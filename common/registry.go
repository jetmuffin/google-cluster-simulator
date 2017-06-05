package common

import (
	"sync"
	log "github.com/Sirupsen/logrus"
	"math"
)

type Registry struct {
	machines     map[int64]*Machine
	machineMutex sync.RWMutex

	jobs             map[int64]*Job
	jobHeap          *PriorityQueue
	jobMutex         sync.RWMutex
	TotalWaitingTime int64 // start_time - submit_time
	TotalRunningTime int64 // end_time - submit_time

	tasks     map[int64]*Task
	taskMutex sync.RWMutex

	events *PriorityQueue
}

func NewRegistry(events *PriorityQueue) *Registry {
	jobHeap := new(PriorityQueue)
	jobHeap.Init(10000000)

	return &Registry{
		machines: make(map[int64]*Machine),
		jobs:     make(map[int64]*Job),
		jobHeap:  jobHeap,
		tasks:    make(map[int64]*Task),
		events:   events,

		TotalRunningTime: 0,
		TotalWaitingTime: 0,
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
	r.jobMutex.RLock()
	defer r.jobMutex.RUnlock()

	if r.jobHeap.Len() == 0 {
		return nil
	}

	item := r.jobHeap.MinItem()
	job, _ := r.jobs[item.Key.(int64)]

	return job
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

func (r *Registry) RunTaskOfJob(job *Job, time int64) *Task {
	r.jobMutex.Lock()
	defer r.jobMutex.Unlock()

	if job.StartTime == 0 {
		job.StartTime = time
	}
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
	r.jobHeap.PushItem(job.JobID, job.Share, []float64{job.Share, -float64(job.taskQueue.Len()), float64(job.SubmitTime)})
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
	r.jobHeap.PushItem(job.JobID, job.Share, []float64{job.Share, -float64(job.taskQueue.Len()), float64(job.SubmitTime)})
}

func (r *Registry) RemoveJob(job *Job, time int64) {
	r.jobMutex.Lock()
	defer r.jobMutex.Unlock()

	if _, ok := r.jobs[job.JobID]; ok {
		job.EndTime = time

		r.TotalWaitingTime += job.StartTime - job.SubmitTime
		r.TotalRunningTime += job.EndTime - job.SubmitTime

		delete(r.jobs, job.JobID)
		r.jobHeap.RemoveItem(job.JobID)
	}
}

func (r *Registry) AddTask(task *Task) {
	r.taskMutex.Lock()
	defer r.taskMutex.Unlock()

	if job, ok := r.jobs[task.JobID]; ok {
		job.taskQueue.PushTask(task)
		job.TaskSubmit ++

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

func (r *Registry) LenEvent() int {
	return r.events.Len()
}

func (r *Registry) PushEvent(event *Event) {
	r.events.PushItem(event.Time, event, []float64{float64(event.Time)})
}

func (r *Registry) PopEvent() *Event {

	return r.events.PopItem().Value.(*Event)
}

func (r *Registry) TopEvent() *Event {
	return r.events.MinItem().Value.(*Event)
}
