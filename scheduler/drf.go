package scheduler

import (
	. "github.com/JetMuffin/google-cluster-simulator/common"
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
	task.Status = TASK_STATUS_FINISHED
	task.EndTime = *d.timeticker
	job := d.registry.GetJob(task.JobID)
	if job != nil {
		d.registry.UpdateJob(job, task, false)
		d.registry.UpdateTask(task)

		if !task.Oversubscribe {
			d.registry.CompleteOnMachine(task)
		}

		//if job.Done() {
		//	d.CompleteJob(job)
		//	log.Infof("[%v] Job %v done(%v/%v)", *d.timeticker/1000/1000, job.JobID, d.jobDone, d.jobNum)
		//}
	}

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
	log.Debugf("[%v] Task(%v) of job(%v) will run at %v", *d.timeticker/1000/1000, task.TaskIndex, task.JobID, *d.timeticker+TIME_DELAY)

	d.registry.PushEvent(&Event{
		EventOrigin:   EVENT_TASK,
		Task:          task,
		Time:          *d.timeticker + task.Duration,
		TaskEventType: TASK_FINISH,
	})
	log.Debugf("[%v] Task(%v) of job(%v) will finished at %v", *d.timeticker/1000/1000, task.TaskIndex, task.JobID, *d.timeticker+task.Duration)

	d.registry.UpdateJob(job, task, true)

	if !task.Oversubscribe {
		d.registry.RunOnMachine(task)
	}
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
	d.registry.CountJainsFairIndex(*d.timeticker)

	job := d.registry.NextScheduleJob()
	if job != nil {
		if d.registry.TaskLenOfJob(job) > 0 {
			stagingTasks := d.registry.WaitingTasks(job)

			if len(stagingTasks) > 0 {
				offers := d.registry.ResourceOffers()
				log.Debugf("Send %v offers to job %v", len(offers), job.JobID)
				result := make(map[int64][]*Task)

				for _, machine := range offers {
					mCpus := machine.Cpus - machine.UsedCpus
					mMem := machine.Mem - machine.UsedCpus

					for _, task := range stagingTasks {
						if task.CpuRequest < mCpus && task.MemoryRequest < mMem {
							result[machine.MachineID] = append(result[machine.MachineID], task)
							mCpus -= task.CpuRequest
							mMem -= task.MemoryRequest
							delete(stagingTasks, task.TaskIndex)
						}
					}
				}

				if len(result) > 0 {
					for machineID, taskList := range result {
						for _, task := range taskList {
							d.runTask(job, task, machineID)
							log.Debugf("[%v] Task(%v) of Job %v run on machine %v", *d.timeticker/1000/1000, task.TaskIndex, job.JobID, task.MachineID)
						}
					}
				} else {
					log.Debugf("No enough resource for job(%v)", job.JobID)
				}
			}
		}
	}
}
