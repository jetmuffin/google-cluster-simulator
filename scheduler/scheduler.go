package scheduler

import (
	. "github.com/JetMuffin/google-cluster-simulator/base"
)

type SchedulerType int

const (
	SCHEDULER_DRF SchedulerType = iota
	SCHEDULER_DATOM
)

type Scheduler interface {
	SubmitJob(job *Job)
	CompleteJob(job *Job)

	SubmitTask(task *Task)
	CompleteTask(task *Task)
	ScheduleTask(task *Task)
	ReportStat()

	AddMachine(machine *Machine)
	RemoveMachine(machine *Machine)
	DeleteMachine(machine *Machine)
	ReportMachine()

	Schedule()
}

