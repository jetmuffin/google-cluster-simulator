package scheduler

import (
	. "github.com/JetMuffin/google-cluster-simulator/common"
	. "github.com/JetMuffin/google-cluster-simulator/monitor"
	log "github.com/Sirupsen/logrus"
	"time"
)

type DRFOScheduler struct {
	monitor *Monitor

	drf DRFScheduler
	Scheduler

	oversubscribeCpu float64
	oversubscribeMem float64
}

func NewDRFOScheduler(monitor *Monitor, registry *Registry, timeticker *int64, signal chan int, jobNum int, taskNum int, cpu float64, mem float64) *DRFOScheduler {
	return &DRFOScheduler{
		monitor: monitor,
		drf: DRFScheduler{
			totalCpu:   cpu,
			totalMem:   mem,
			jobNum:     jobNum,
			taskNum:    taskNum,
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
	for {
		flag := d.ScheduleOnce()
		if !flag {
			break
		}
	}
}

func (d *DRFOScheduler) runOversubscribeTask(job *Job, be *Task, machineID int64) {
	be.Oversubscribe = true
	d.drf.runTask(job, be, machineID)
}

func (d *DRFOScheduler) ScheduleOnce() bool {
	defer d.drf.registry.TimeCost(time.Now())
	return d.scheduleOnNormalQueue() || d.scheduleOnOversubscribedQueue()
}

func (d *DRFOScheduler) scheduleOnNormalQueue() bool {
	return d.drf.ScheduleOnce()
}

func (d *DRFOScheduler) scheduleOnOversubscribedQueue() bool {
	job := d.drf.registry.NextScheduleJob()

	if job != nil {
		if d.drf.registry.TaskLenOfJob(job) > 0 {
			stagingTasks := d.drf.registry.WaitingTasks(job)

			if len(stagingTasks) > 0 {
				offers := d.monitor.ResourceOffers()
				for _, machine := range offers {
					oCpus := machine.OversubscribedCpus - machine.UsedOversubscribedCpus
					oMem := machine.OversubscribedMem - machine.UsedOversubscribedMem
					//log.Debugf("Revocable offer: %v %v %v %v" , oCpus, oMem, machine.OversubscribedCpus, machine.UsedOversubscribedCpus)
					for _, task := range stagingTasks {
						if task.CpuRequest < oCpus && task.MemoryRequest < oMem {
							d.runOversubscribeTask(job, task, machine.MachineID)
							log.Debugf("[%v] Schedule oversubscribed task(%v) of Job %v run on machine %v", *d.drf.timeticker/1000/1000, task.TaskIndex, job.JobID, task.MachineID)
							return true
						}
					}
				}

				//log.Debugf("No enough oversubscribed resource for job(%v)", job.JobID)
			} else {
				//log.Debug("Job has no task to run.")
			}
		} else {
			//log.Debug("No job to run.")
		}
	}
	return false
}
