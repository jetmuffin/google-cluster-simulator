package scheduler

import (
	. "github.com/JetMuffin/google-cluster-simulator/common"
	. "github.com/JetMuffin/google-cluster-simulator/monitor"
	log "github.com/Sirupsen/logrus"
)

type DRFOScheduler struct {
	monitor *Monitor

	drf DRFScheduler
	Scheduler

	oversubscribeCpu float64
	oversubscribeMem float64
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

	if task.Oversubscribe {
		d.oversubscribeCpu -= task.CpuRequest
		d.oversubscribeMem -= task.MemoryRequest
	}
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

func (d *DRFOScheduler) runOversubscribeTask(job *Job, be *Task) {
	be.Oversubscribe = true
	d.drf.runTask(job, be)

	d.oversubscribeCpu += be.CpuRequest
	d.oversubscribeMem += be.MemoryRequest
}

func (d *DRFOScheduler) ScheduleOnce() {
	job := d.drf.registry.NextScheduleJob()
	if job != nil {
		if d.drf.registry.TaskLenOfJob(job) > 0 {
			task := d.drf.registry.GetFirstTaskOfJob(job)

			if task != nil && task.CpuRequest < d.drf.totalCpu && task.MemoryRequest < d.drf.totalMem {
				d.drf.runTask(job, task)
				d.drf.registry.CountJainsFairIndex()

				log.Debugf("[%v] %v tasks of Job %v run, resource available(%v %v)", *d.drf.timeticker/1000/1000, task.TaskIndex, job.JobID, d.drf.totalCpu, d.drf.totalMem)
			} else {
				log.Debugf("No enough resource for task(%v) job(%v), request(%v %v), available(%v %v)", task.TaskIndex, task.JobID, task.CpuRequest, task.MemoryRequest, d.drf.totalCpu, d.drf.totalMem)
				slackCpu, slackMem := d.monitor.RunOnce()

				log.Debugf("Revocable resource: cpu(%v/%v), mem(%v/%v)", slackCpu - d.oversubscribeCpu, slackCpu, slackMem - d.oversubscribeMem, slackMem)
				if task.CpuRequest < (slackCpu - d.oversubscribeCpu) && task.MemoryRequest < (slackMem - d.oversubscribeMem) {
					d.runOversubscribeTask(job, task)
					d.drf.registry.CountJainsFairIndex()
				}
			}
		}
	}
}
