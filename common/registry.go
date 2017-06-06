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
	}
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

func (r *Registry) GetFirstTaskOfJob(job *Job) *Task {
	j := r.jobs.GetItem(job.JobID)

	if j != nil && job.taskQueue.Len() > 0 {
		return job.taskQueue.Peek()
	}
	return nil
}

func (r *Registry) TaskLenOfJob(job *Job) int {
	j := r.jobs.GetItem(job.JobID)
	if j != nil {
		return j.taskQueue.Len()
	}
	return 0
}

func (r *Registry) RunTaskOfJob(job *Job, time int64) *Task {
	if job.StartTime == 0 {
		job.StartTime = time
	}
	task := job.taskQueue.PopTask()

	return task
}

func (r *Registry) JobLen() int {
	return r.jobs.Len()
}

func (r *Registry) AddJob(job *Job) {
	r.jobs.PushItem(job.JobID, job)
}

func (r *Registry) UpdateJob(job *Job, task *Task, totalCpu, totalMem float64, add bool) {
	if add {
		job.CpuUsed += task.CpuRequest
		job.MemUsed += task.MemoryRequest
	} else {
		job.CpuUsed -= task.CpuRequest
		job.MemUsed -= task.MemoryRequest
		job.TaskDone ++
	}
	job.Share = math.Max(job.CpuUsed/totalCpu, job.MemUsed/totalMem)
	r.jobs.UpdateItem(job.JobID, job)
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
		job.taskQueue.PushTask(task)
		r.tasks[TaskID(task)] = task

		r.jobs.UpdateItem(job.JobID, job)
	}
}

func (r *Registry) AddRunningService(task *Task) {
	if task.Duration < 900000000 || task.Status != TASK_STATUS_RUNNING {
		return
	}
	r.taskMutex.Lock()
	defer r.taskMutex.Unlock()

	r.runningService[TaskID(task)] = task
}

func (r *Registry) GetRunningService() []*Task {
	r.taskMutex.RLock()
	defer r.taskMutex.RUnlock()

	var services []*Task
	for _, t := range r.runningService {
		services = append(services, t)
	}

	return services
}

func (r *Registry) RemoveRunningService(task *Task) {
	if task.Duration < 900000000 || task.Status != TASK_STATUS_RUNNING {
		return
	}
	r.taskMutex.Lock()
	defer r.taskMutex.Unlock()


	if _, ok := r.runningService[TaskID(task)]; ok {
		delete(r.runningService, TaskID(task))
	} else {
		log.Errorf("Task not found when remove running service: job(%v) index(%v)", task.JobID, task.TaskIndex)
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
