package simulator

import (
	. "github.com/JetMuffin/google-cluster-simulator/scheduler"
	. "github.com/JetMuffin/google-cluster-simulator/common"
	. "github.com/JetMuffin/google-cluster-simulator/monitor"
	log "github.com/Sirupsen/logrus"
	"errors"
)

type Simulator struct {
	loader    *TraceLoader
	scheduler Scheduler
	monitor   *Monitor
	registry  *Registry
	config    Config

	jobNum     int
	taskNum    int
	machineNum int

	timeticker *int64
	signal     chan int
}

func NewSimulator(config Config) (*Simulator, error) {
	s := &Simulator{
		loader:     NewLoader(config.Directory),
		timeticker: new(int64),
		signal:     make(chan int, 1),
		config:     config,
	}

	events, machineNum, jobNum, taskNum, err := s.loader.LoadEvents()
	s.taskNum = taskNum
	s.machineNum = machineNum
	s.jobNum = jobNum

	usage, err := s.loader.LoadUsages()
	if err != nil {
		return nil, err
	}

	log.Infof("Load %v events from %v", len(events), config.Directory)

	eventHeap := NewEventHeap(events)
	s.registry = NewRegistry(&eventHeap)

	s.monitor = NewMonitor(usage, s.registry, NewMonitorParam(config.Alpha, config.Beta, config.Theta, config.Lambda, config.Gamma), s.timeticker)

	switch SchedulerType(config.Scheduler) {
	case SCHEDULER_DRF:
		s.scheduler = NewDRFScheduler(s.registry, s.timeticker, s.signal, jobNum, taskNum, config.Cpu, config.Mem)
		break
	case SCHEDULER_DATOM:
		s.scheduler = NewDRFOScheduler(s.monitor, s.registry, s.timeticker, s.signal, jobNum, taskNum, config.Cpu, config.Mem)
		break
	default:
		return nil, errors.New("Unknown scheduler type")
	}

	return s, nil
}

func (s *Simulator) HandleMachineEvent(event *Event) {
	machine := event.Machine

	switch event.MachineEventType {
	case MACHINE_ADD:
		//log.Debugf("[%v] Add a new machine %v with cpu(%v) mem(%v)", event.Time/1000/1000, machine.MachineID, machine.Cpus, machine.Mem)
		s.registry.AddMachine(machine)
	case MACHINE_REMOVE:
		//log.Debugf("[%v] Remove a machine %v with cpu(%v) mem(%v)", event.Time/1000/1000, machine.MachineID, machine.Cpus, machine.Mem)
		s.registry.RemoveMachine(machine)
	case MACHINE_UPDATE:
		//log.Debugf("[%v] Update a machine %v with cpu(%v) mem(%v)", event.Time/1000/1000, machine.MachineID, machine.Cpus, machine.Mem)
		s.registry.UpdateMachine(machine)
	}
	if event.Time > 0 {
		s.scheduler.Schedule()
	}
}

func (s *Simulator) HandleJobEvent(event *Event) {
	job := event.Job

	switch event.TaskEventType {
	case TASK_SUBMIT:
		log.Debugf("[%v] Submit new job(%v)", event.Time/1000/1000, job.JobID)
		s.scheduler.SubmitJob(job)
	case TASK_FINISH:
		log.Debugf("[%v] Job(%v) done %v", event.Time/1000/1000, job.JobID, s.scheduler.Progress())
		s.scheduler.CompleteJob(job)
	default:
		log.Errorf("Unknown job event type %v", event.TaskEventType)
	}
	s.scheduler.Schedule()
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
		log.Errorf("Unknown task event type: %v", event.TaskEventType)
	}
	s.scheduler.Schedule()
}

func (s *Simulator) statistic() *Statistics{
	allDoneTime := *s.timeticker / 1000 / 1000

	return &Statistics{
		TaskNum: int64(s.taskNum),
		JobNum: int64(s.jobNum),
		MachineNum: int64(s.machineNum),
		JobFinishTime: float64(allDoneTime),
		FairIndex: s.registry.GetJainsFairIndex(*s.timeticker),
		Overhead: s.registry.AverageTimeCost(),
		Throughput: s.registry.GetThroughput(*s.timeticker),
		Makespan: s.registry.GetMakespan(),
	}
}

func (s *Simulator) Run() *Statistics {
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
			case EVENT_MACHINE:
				s.HandleMachineEvent(event)
				break
			}
		}
	}
	log.Debug("Done")
	return s.statistic()
}
