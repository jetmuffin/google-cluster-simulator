package scheduler

import (
	. "github.com/JetMuffin/google-cluster-simulator/base"
	. "github.com/JetMuffin/google-cluster-simulator/monitor"
	log "github.com/Sirupsen/logrus"
)

type DRFOScheduler struct {
	monitor *Monitor

	drf DRFScheduler
	Scheduler
}

func NewDRFOScheduler(monitor *Monitor, registry *Registry, timeticker *int64, signal chan int, jobNum int, cpu float64, mem float64) *DRFOScheduler {
	return &DRFOScheduler{
		monitor: monitor,
		drf: DRFScheduler{
			totalCpu:   cpu,
			totalMem:   mem,
			jobNum:     jobNum,
			timeticker: timeticker,
			signal:     signal,
			registry:   registry,
		},
	}
}

func (d *DRFOScheduler) SubmitJob(job *Job) {
	d.drf.SubmitJob(job)
}

func (d *DRFOScheduler) CompleteJob(job *Job) {
	d.drf.CompleteJob(job)
}

func (d *DRFOScheduler) SubmitTask(task *Task) {
	d.drf.SubmitTask(task)
}

func (d *DRFOScheduler) ScheduleTask(task *Task) {
	d.drf.ScheduleTask(task)
}

func (d *DRFOScheduler) CompleteTask(task *Task) {
	d.drf.CompleteTask(task)
}

func (d *DRFOScheduler) Progress() string {
	return d.drf.Progress()
}

func (d *DRFOScheduler) Done() bool {
	return d.drf.Done()
}

func (d *DRFOScheduler) Schedule() {
	go func() {
		for {
			d.ScheduleOnce()
		}
	}()
}

func (d *DRFOScheduler) runOversubscribeTask(job *Job, be *Task, lrs *Task) {
	be.Oversubscribe = true
	d.drf.runTask(job, be)

	lrs.CpuSlack -= be.CpuRequest
	lrs.MemSlack -= be.CpuRequest
	d.drf.registry.UpdateTask(lrs)
}

func (d *DRFOScheduler) ScheduleOnce() {
	job := d.drf.registry.NextScheduleJob()
	if job != nil {
		if d.drf.registry.TaskLenOfJob(job) > 0 {
			task := d.drf.registry.GetFirstTaskOfJob(job)

			if task != nil && task.CpuRequest < d.drf.totalCpu && task.MemoryRequest < d.drf.totalMem {
				d.drf.runTask(job, task)

				log.Debugf("[%v] %v tasks of Job %v run, resource available(%v %v)", *d.drf.timeticker/1000/1000, task.TaskIndex, job.JobID, d.drf.totalCpu, d.drf.totalMem)
			} else {
				log.Debugf("No enough resource for task(%v) job(%v), request(%v %v), available(%v %v)", task.TaskIndex, task.JobID, task.CpuRequest, task.MemoryRequest, d.drf.totalCpu, d.drf.totalMem)
				oversubscribeCandidates := d.drf.registry.FilterTask(func(task *Task) bool {
					return task.Status == TASK_STATUS_RUNNING && task.Duration > 600000000 && task.CpuSlack > 0 && task.MemSlack > 0
				})

				log.Debugf("Found %d candidates for oversubscription", len(oversubscribeCandidates))
				for _, oversubscribeTask := range oversubscribeCandidates {
					if oversubscribeTask.CpuSlack > task.CpuRequest && oversubscribeTask.MemSlack > task.MemoryRequest {
						d.runOversubscribeTask(job, task, oversubscribeTask)
						log.Debugf("Oversubscribe on task(%v)-job(%v) for task(%v)-job(%v)", oversubscribeTask.TaskIndex, oversubscribeTask.JobID, task.TaskIndex, task.JobID)
						break
					}
				}
			}
		}
	}
}
