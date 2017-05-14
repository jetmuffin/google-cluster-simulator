package simulator

import (
	. "github.com/JetMuffin/google-cluster-simulator/scheduler"
	. "github.com/JetMuffin/google-cluster-simulator/base"
	log "github.com/Sirupsen/logrus"
)

type Simulator struct {
	loader    *TraceLoader
	scheduler Scheduler
	registry  *Registry

	timeticker *int64
	signal chan int
}

func NewSimulator(directory string, schedulerType SchedulerType) (*Simulator, error) {
	s := &Simulator{
		loader:     NewLoader(directory),
		timeticker: new(int64),
		signal: make(chan int, 1),
	}

	events, err := s.loader.LoadMarshalEvents()
	if err != nil {
		return nil, err
	}

	eventHeap := NewEventHeap(events)
	s.registry = NewRegistry(&eventHeap)

	s.scheduler = NewDRFScheduler(s.registry, s.timeticker, s.signal,0.5, 10, 589)

	return s, nil
}

func (s *Simulator) HandleJobEvent(event *Event) {
	job := event.Job

	switch event.TaskEventType {
	case TASK_SUBMIT:
		log.Infof("[%v] Submit new job(%v) with %v tasks", event.Time/1000/1000, event.Job.JobID, event.Job.TaskNum)
		s.scheduler.SubmitJob(job)
	case TASK_FINISH:
		log.Infof("[%v] Job(%v) done %v", event.Time/1000/1000, event.Job.JobID, s.scheduler.Progress())
		s.scheduler.CompleteJob(job)
	default:
		log.Error("Unknown event type")
	}
	s.scheduler.ScheduleOnce()
}

func (s *Simulator) HandleTaskEvent(event *Event) {
	task := event.Task
	switch event.TaskEventType {
	case TASK_SUBMIT:
		log.Infof("[%v] Submit new task(%v) of job(%v) request for cpu(%v) mem(%v)", event.Time/1000/1000, task.TaskIndex, task.JobID, task.CpuRequest, task.MemoryRequest)
		s.scheduler.SubmitTask(task)
	case TASK_SCHEDULE:
		log.Infof("[%v] Task(%v) of job(%v) begin to run", event.Time/1000/1000, task.TaskIndex, task.JobID)
		s.scheduler.ScheduleTask(task)
	case TASK_FINISH:
		log.Infof("[%v] Task(%v) of job(%v) is finished", event.Time/1000/1000, task.TaskIndex, task.JobID)
		s.scheduler.CompleteTask(task)
	default:
		log.Error("Unknown event type")
	}
	s.scheduler.ScheduleOnce()
}

func (s *Simulator) Run() {
	for !s.scheduler.Done() {
		if s.registry.LenEvent() > 0 {
			event := s.registry.PopEvent()
			if event.Time > *s.timeticker {
				*s.timeticker = event.Time + TIME_DELAY
			} else {
				*s.timeticker = *s.timeticker + TIME_DELAY
			}

			switch event.EventOrigin {
			case EVENT_TASK:
				s.HandleTaskEvent(event)
				break
			case EVENT_JOB:
				s.HandleJobEvent(event)
				break
			}
		}
	}

	log.Debug("Done")
	archive := s.registry.ArchiveTask()
	log.Infof("Task staging: %v, task running: %v, task finished: %v", len(archive[TASK_STATUS_STAGING]), len(archive[TASK_STATUS_RUNNING]), len(archive[TASK_STATUS_FINISHED]))
}
