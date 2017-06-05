package simulator

import (
	. "github.com/JetMuffin/google-cluster-simulator/scheduler"
	. "github.com/JetMuffin/google-cluster-simulator/base"
	. "github.com/JetMuffin/google-cluster-simulator/monitor"
	log "github.com/Sirupsen/logrus"
	"errors"
)

type Simulator struct {
	loader    *TraceLoader
	scheduler Scheduler
	monitor   *Monitor
	registry  *Registry

	jobNum int

	timeticker *int64
	signal     chan int
}

func NewSimulator(config Config) (*Simulator, error) {
	s := &Simulator{
		loader:     NewLoader(config.Directory),
		timeticker: new(int64),
		signal:     make(chan int, 1),
	}

	events, jobNum, err := s.loader.LoadMarshalEvents()
	usage, err := s.loader.LoadUsage()
	if err != nil {
		return nil, err
	}

	eventHeap := NewEventHeap(events)
	s.registry = NewRegistry(&eventHeap)

	s.monitor = NewMonitor(usage, s.registry, NewMonitorParam(config.Alpha, config.Beta, config.Theta, config.Lambda, config.Gamma), s.timeticker)
	s.jobNum = jobNum

	switch SchedulerType(config.Scheduler) {
	case SCHEDULER_DRF:
		s.scheduler = NewDRFScheduler(s.registry, s.timeticker, s.signal, jobNum, config.Cpu, config.Mem)
		break
	case SCHEDULER_DATOM:
		s.scheduler = NewDRFOScheduler(s.monitor, s.registry, s.timeticker, s.signal, jobNum, config.Cpu, config.Mem)
		break
	default:
		return nil, errors.New("Unknown scheduler type")
	}

	return s, nil
}

func (s *Simulator) HandleJobEvent(event *Event) {
	job := event.Job

	switch event.TaskEventType {
	case TASK_SUBMIT:
		log.Debugf("[%v] Submit new job(%v) with %v tasks", event.Time/1000/1000, event.Job.JobID, event.Job.TaskNum)
		s.scheduler.SubmitJob(job)
	case TASK_FINISH:
		log.Debugf("[%v] Job(%v) done %v", event.Time/1000/1000, event.Job.JobID, s.scheduler.Progress())
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
		log.Debugf("[%v] Submit new task(%v) of job(%v) request for cpu(%v) mem(%v)", event.Time/1000/1000, task.TaskIndex, task.JobID, task.CpuRequest, task.MemoryRequest)
		s.scheduler.SubmitTask(task)
	case TASK_SCHEDULE:
		log.Debugf("[%v] Task(%v) of job(%v) begin to run", event.Time/1000/1000, task.TaskIndex, task.JobID)
		s.scheduler.ScheduleTask(task)
	case TASK_FINISH:
		log.Debugf("[%v] Task(%v) of job(%v) is finished", event.Time/1000/1000, task.TaskIndex, task.JobID)
		s.scheduler.CompleteTask(task)
	default:
		log.Error("Unknown event type")
	}
	s.scheduler.ScheduleOnce()
}

func (s *Simulator) statistic() {
	averageWaitingTime := float64(s.registry.TotalWaitingTime) / 1000.0 / 1000.0 / float64(s.jobNum)
	averageRunningTime := float64(s.registry.TotalRunningTime) / 1000.0 / 1000.0 / float64(s.jobNum)
	allDoneTime := *s.timeticker / 1000 / 1000

	log.Infof("Average running time: %v, average waiting time: %v, all job finished time: %v", averageRunningTime, averageWaitingTime, allDoneTime)
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

	s.statistic()
}
