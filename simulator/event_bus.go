package simulator

import (
	"errors"
	. "github.com/JetMuffin/google-cluster-simulator/base"
	. "github.com/JetMuffin/google-cluster-simulator/scheduler"
	log "github.com/Sirupsen/logrus"
)

type EventBus struct {
	eventchans EventChans
	done       chan int

	scheduler Scheduler
}

func NewEventBus(scheduler Scheduler) *EventBus {
	return &EventBus{
		eventchans: EventChans{
			EVENT_MACHINE: make(chan *Event),
			EVENT_JOB:     make(chan *Event),
			EVENT_TASK:    make(chan *Event),
		},
		done:      make(chan int),
		scheduler: scheduler,
	}
}

func (e *EventBus) AddEvent(event *Event) error {
	if c, ok := e.eventchans[event.EventOrigin]; ok {
		c <- event
		return nil
	} else {
		return errors.New("Unknown event origin")
	}
}

func (e *EventBus) HandleMachinEvent(event *Event) {
	e.scheduler.AddMachine(event.Machine)
	//log.Infof("[%v] Add new machine(%v) with cpus(%v) mem(%v)", event.Time/1000/1000, event.Machine.MachineID, event.Machine.Cpus, event.Machine.Mem)
}

func (e *EventBus) HandleJobEvent(event *Event) {
	job := event.Job

	switch event.TaskEventType {
	case TASK_SUBMIT:
		log.Infof("[%v] Submit new job(%v) with %v tasks", event.Time/1000/1000, event.Job.JobID, event.Job.TaskNum)
		e.scheduler.SubmitJob(job)
	case TASK_FINISH:
		log.Infof("[%v] Job(%v) done", event.Time/1000/1000, event.Job.JobID)
		e.scheduler.CompleteJob(job)
	default:
		log.Error("Unknown event type")
	}
}

func (e *EventBus) HandleTaskEvent(event *Event) {
	task := event.Task
	switch event.TaskEventType {
	case TASK_SUBMIT:
		log.Infof("[%v] Submit new task(%v) of job(%v) request for cpu(%v) mem(%v)", event.Time/1000/1000, task.TaskIndex, task.JobID, task.CpuRequest, task.MemoryRequest)
		e.scheduler.SubmitTask(task)
	case TASK_SCHEDULE:
		log.Infof("[%v] Task(%v) of job(%v) begin to run", event.Time/1000/1000, task.TaskIndex, task.JobID)
		e.scheduler.ScheduleTask(task)
	case TASK_FINISH:
		log.Infof("[%v] Task(%v) of job(%v) is finished", event.Time/1000/1000, task.TaskIndex, task.JobID)
		e.scheduler.CompleteTask(task)
	default:
		log.Error("Unknown event type")
	}
}

func (e *EventBus) Done() {
	e.done <- 0
}

func (e *EventBus) Listen() {
	go func() {
		for {
			select {
			case event := <-e.eventchans[EVENT_MACHINE]:
				e.HandleMachinEvent(event)
			case event := <-e.eventchans[EVENT_JOB]:
				e.HandleJobEvent(event)
			case event := <-e.eventchans[EVENT_TASK]:
				e.HandleTaskEvent(event)
			case <-e.done:
				return
			}
		}
	}()
}
