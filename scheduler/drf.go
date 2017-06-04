package scheduler

import (
	. "github.com/JetMuffin/google-cluster-simulator/base"
	log "github.com/Sirupsen/logrus"
	"fmt"
)

type DRFScheduler struct {
	totalCpu float64
	totalMem float64
	jobDone  int
	jobNum   int

	timeticker *int64
	signal     chan int

	registry *Registry

	Scheduler
}

func NewDRFScheduler(registry *Registry, timeticker *int64, signal chan int, jobNum int, cpu float64, mem float64) *DRFScheduler {
	return &DRFScheduler{
		totalCpu:   cpu,
		totalMem:   mem,
		jobNum:     jobNum,
		timeticker: timeticker,
		signal:     signal,
		registry:   registry,
	}
}

func (d *DRFScheduler) SubmitJob(job *Job) {
	d.registry.AddJob(job)
}

func (d *DRFScheduler) CompleteJob(job *Job) {
	d.registry.RemoveJob(job, *d.timeticker)
	d.jobDone++
}

func (d *DRFScheduler) SubmitTask(task *Task) {
	task.Status = TASK_STATUS_STAGING
	d.registry.AddTask(task)
}

func (d *DRFScheduler) ScheduleTask(task *Task) {
	task.Status = TASK_STATUS_RUNNING
	task.StartTime = *d.timeticker
	d.registry.UpdateTask(task)
}

func (d *DRFScheduler) CompleteTask(task *Task) {
	//d.registry.RemoveTask(task)
	task.Status = TASK_STATUS_FINISHED
	task.EndTime = *d.timeticker
	job := d.registry.GetJob(task.JobID)
	if job != nil {
		d.registry.UpdateJob(job, task, d.totalCpu, d.totalMem, false)
		d.registry.UpdateTask(task)

		d.totalCpu += task.CpuRequest
		d.totalMem += task.MemoryRequest

		if job.Done() {
			d.CompleteJob(job)
			log.Infof("[%v] Job %v done(%v/%v)", *d.timeticker/1000/1000, job.JobID, d.jobDone, d.jobNum)
		}
	}
}

func (d *DRFScheduler) runTask(job *Job, task *Task) {
	d.registry.PushEvent(&Event{
		EventOrigin:   EVENT_TASK,
		Task:          task,
		Time:          *d.timeticker + TIME_DELAY,
		TaskEventType: TASK_SCHEDULE,
	})
	log.Debugf("[%v] Task(%v) of job(%v) will run at %v", *d.timeticker/1000/1000, task.TaskIndex, task.JobID, *d.timeticker+TIME_DELAY)

	d.registry.PushEvent(&Event{
		EventOrigin:   EVENT_TASK,
		Task:          task,
		Time:          *d.timeticker + task.Duration,
		TaskEventType: TASK_FINISH,
	})
	log.Debugf("[%v] Task(%v) of job(%v) will finished at %v", *d.timeticker/1000/1000, task.TaskIndex, task.JobID, *d.timeticker+task.Duration)

	d.registry.UpdateJob(job, task, d.totalCpu, d.totalMem, true)
	d.registry.RunTaskOfJob(job, *d.timeticker)

	d.totalCpu -= task.CpuRequest
	d.totalMem -= task.MemoryRequest
}

func (d *DRFScheduler) Progress() string {
	return fmt.Sprintf("(%v/%v)", d.jobDone, d.jobNum)
}

func (d *DRFScheduler) Done() bool {
	return d.jobDone == d.jobNum
}

func (d *DRFScheduler) Schedule() {
	go func() {
		for {
			d.ScheduleOnce()
		}
	}()
}

func (d *DRFScheduler) ScheduleOnce() {
	job := d.registry.NextScheduleJob()
	if job != nil {
		if d.registry.TaskLenOfJob(job) > 0 {
			task := d.registry.GetFirstTaskOfJob(job)

			if task != nil && task.CpuRequest < d.totalCpu && task.MemoryRequest < d.totalMem {
				d.runTask(job, task)

				log.Debugf("[%v] %v tasks of Job %v run, resource available(%v %v)", *d.timeticker/1000/1000, task.TaskIndex, job.JobID, d.totalCpu, d.totalMem)
			} else {
				log.Debugf("No enough resource for task(%v) job(%v), request(%v %v), available(%v %v)", task.TaskIndex, task.JobID, task.CpuRequest, task.MemoryRequest, d.totalCpu, d.totalMem)
			}
		}
	}
}
