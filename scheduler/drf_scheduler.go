package scheduler

import (
	. "github.com/JetMuffin/google-cluster-simulator/base"
	log "github.com/Sirupsen/logrus"
	"sync"
	"time"
)

const (
	TIME_DELAY = 100
)

type DRFScheduler struct {
	totalCpu   float64
	totalMem   float64
	timeticker *int64
	timeMutex  sync.Mutex

	registry *Registry
	heap     JobHeap

	Scheduler
}

func NewDRFScheduler(registry *Registry, timeticker *int64, timeMutex sync.Mutex, cpu float64, mem float64) *DRFScheduler {
	return &DRFScheduler{
		totalCpu:   cpu,
		totalMem:   mem,
		timeticker: timeticker,
		timeMutex:  timeMutex,
		registry:   registry,
		heap:       NewJobHeap(),
	}
}

func (d *DRFScheduler) SubmitJob(job *Job) {
	d.registry.AddJob(job)
	d.heap.PushJob(job)
}

func (d *DRFScheduler) CompleteJob(job *Job) {
	//d.registry.RemoveJob(job)
	d.heap.PopJob()
}

func (d *DRFScheduler) SubmitTask(task *Task) {
	task.Status = TASK_STATUS_STAGING
	d.registry.AddTask(task)

	d.Schedule()
}

func (d *DRFScheduler) ScheduleTask(task *Task) {
	task.Status = TASK_STATUS_RUNNING
	job := d.registry.GetJob(task.JobID)
	if job != nil {
		job.UpdateStat(task, d.totalCpu, d.totalMem, true)
		d.registry.UpdateTask(task)
		d.heap.UpdateShare(job, job.Share)

		d.totalCpu -= task.CpuRequest
		d.totalMem -= task.MemoryRequest

	}
}

func (d *DRFScheduler) CompleteTask(task *Task) {
	//d.registry.RemoveTask(task)
	task.Status = TASK_STATUS_FINISHED
	job := d.registry.GetJob(task.JobID)
	if job != nil {
		job.UpdateStat(task, d.totalCpu, d.totalMem, false)
		d.registry.UpdateTask(task)
		d.heap.UpdateShare(job, job.Share)

		d.totalCpu += task.CpuRequest
		d.totalMem += task.MemoryRequest

		if job.Done() {
			d.CompleteJob(job)
			log.Debugf("Job(%v) complete with %v tasks", job.JobID, job.TaskNum)
		}
	}
}

func (d *DRFScheduler) nextScheduleJob() *Job {
	return d.heap.TopJob()
}

func (d *DRFScheduler) Schedule() {
	go func() {
		for {
			job := d.nextScheduleJob()
			if job != nil {
				if d.registry.TaskLenOfJob(job) > 0 {
					task := d.registry.GetFirstTaskOfJob(job)
					if task == nil {
						continue
					}

					if task.CpuRequest < d.totalCpu && task.MemoryRequest < d.totalMem {
						d.timeMutex.Lock()

						// schedule task
						d.registry.PushEvent(&Event{
							EventOrigin:   EVENT_TASK,
							Task:          task,
							Time:          *d.timeticker + TIME_DELAY,
							TaskEventType: TASK_SCHEDULE,
						})
						log.Debugf("Task(%v) of job(%v) will run at %v", task.TaskIndex, task.JobID, *d.timeticker+TIME_DELAY)

						// complete task
						d.registry.PushEvent(&Event{
							EventOrigin:   EVENT_TASK,
							Task:          task,
							Time:          *d.timeticker + task.Duration,
							TaskEventType: TASK_FINISH,
						})

						log.Debugf("Task(%v) of job(%v) will finished at %v", task.TaskIndex, task.JobID, *d.timeticker+task.Duration)
						d.timeMutex.Unlock()

					} else {
						log.Warnf("No enough resource for task(%v) job(%v)", task.TaskIndex, task.JobID)
					}
				} else if job.Done() {
					d.CompleteJob(job)
				}
			}

			time.Sleep(100 * time.Millisecond)
		}
	}()

}
