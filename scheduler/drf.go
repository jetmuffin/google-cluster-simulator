package scheduler

import (
	. "github.com/JetMuffin/google-cluster-simulator/common"
	log "github.com/Sirupsen/logrus"
	"fmt"
	"time"
)

type DRFScheduler struct {
	totalCpu float64
	totalMem float64
	jobDone  int
	taskDone int
	jobNum   int
	taskNum  int

	timeticker *int64
	signal     chan int

	registry *Registry

	Scheduler
}

func NewDRFScheduler(registry *Registry, timeticker *int64, signal chan int, jobNum int, taskNum int, cpu float64, mem float64) *DRFScheduler {
	return &DRFScheduler{
		totalCpu:   cpu,
		totalMem:   mem,
		jobNum:     jobNum,
		taskNum:    taskNum,
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
	task.SubmitTime = *d.timeticker
	d.registry.AddTask(task)
}

func (d *DRFScheduler) ScheduleTask(task *Task) {
	task.Status = TASK_STATUS_RUNNING
	task.StartTime = *d.timeticker
	d.registry.UpdateTask(task)
}

func (d *DRFScheduler) CompleteTask(task *Task) {
	task.Status = TASK_STATUS_FINISHED
	task.EndTime = *d.timeticker
	d.registry.UpdateMakespan(task)

	job := d.registry.GetJob(task.JobID)
	if job != nil {
		d.registry.UpdateJob(job, task, false)
		d.registry.UpdateTask(task)

		d.registry.CompleteOnMachine(task)

		if job.Done() {
			d.CompleteJob(job)
			log.Infof("[%v] Job %v done(%v/%v)", *d.timeticker/1000/1000, job.JobID, d.jobDone, d.jobNum)
		}
	}

	d.taskDone ++
	log.Infof("[%v] Task %v done(%v/%v)", *d.timeticker/1000/1000, GetTaskID(task.JobID, task.TaskIndex), d.taskDone, d.taskNum)

	d.registry.RemoveTask(task)
}

func (d *DRFScheduler) runTask(job *Job, task *Task, machineID int64) {
	task.MachineID = machineID
	task.Status = TASK_STATUS_RUNNING
	d.registry.PushEvent(&Event{
		EventOrigin:   EVENT_TASK,
		Task:          task,
		Time:          *d.timeticker + TIME_DELAY,
		TaskEventType: TASK_SCHEDULE,
	})
	//log.Debugf("[%v] Task(%v) of job(%v) will run at %v", *d.timeticker/1000/1000, task.TaskIndex, task.JobID, *d.timeticker+TIME_DELAY)

	d.registry.PushEvent(&Event{
		EventOrigin:   EVENT_TASK,
		Task:          task,
		Time:          *d.timeticker + task.Duration,
		TaskEventType: TASK_FINISH,
	})
	//log.Debugf("[%v] Task(%v) of job(%v) will finished at %v", *d.timeticker/1000/1000, task.TaskIndex, task.JobID, *d.timeticker+task.Duration)

	d.registry.UpdateJob(job, task, true)

	d.registry.RunOnMachine(task)
}

func (d *DRFScheduler) Progress() string {
	return fmt.Sprintf("(%v/%v)", d.jobDone, d.jobNum)
}

func (d *DRFScheduler) Done() bool {
	return d.jobDone == d.jobNum
}

func (d *DRFScheduler) Schedule() {
	defer d.registry.TimeCost(time.Now())
	for {
		flag := d.ScheduleOnce()
		if !flag {
			break
		}
	}
}

func (d *DRFScheduler) ScheduleOnce() bool {
	d.registry.CountJainsFairIndex(*d.timeticker)

	job := d.registry.NextScheduleJob()
	if job != nil {
		if d.registry.TaskLenOfJob(job) > 0 {
			stagingTasks := d.registry.WaitingTasks(job)

			if len(stagingTasks) > 0 {
				offers := d.registry.ResourceOffers()

				for _, machine := range offers {
					mCpus := machine.Cpus - machine.UsedCpus
					mMem := machine.Mem - machine.UsedCpus

					for _, task := range stagingTasks {
						if task.CpuRequest < mCpus && task.MemoryRequest < mMem {
							d.runTask(job, task, machine.MachineID)
							log.Debugf("[%v] Schedule task(%v) of Job %v run on machine %v", *d.timeticker/1000/1000, task.TaskIndex, job.JobID, task.MachineID)
							return true
						}
					}
				}

				log.Debugf("No enough resource for job(%v)", job.JobID)
			}
		} else {
			log.Debug("Job has no task to run.")
		}
	} else {
		log.Debug("No job to run.")
	}

	return false
}
