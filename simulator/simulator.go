package simulator

import (
	. "github.com/JetMuffin/google-cluster-simulator/scheduler"
	. "github.com/JetMuffin/google-cluster-simulator/base"
	log "github.com/Sirupsen/logrus"
	"sync"
)

type Simulator struct {
	loader    *TraceLoader
	scheduler Scheduler
	bus       *EventBus
	registry  *Registry

	timeticker *int64
	timeMutex sync.Mutex
}

func NewSimulator(directory string, schedulerType SchedulerType) (*Simulator, error) {
	simulator := &Simulator{
		loader:     NewLoader(directory),
		timeticker: new(int64),
	}

	events, err := simulator.loader.LoadMarshalEvents()
	if err != nil {
		return nil, err
	}

	eventHeap := NewEventHeap(events)
	simulator.registry = NewRegistry(&eventHeap)

	simulator.scheduler = NewDRFScheduler(simulator.registry, simulator.timeticker, simulator.timeMutex,4, 10)
	simulator.bus = NewEventBus(simulator.scheduler)

	return simulator, nil
}

func (s *Simulator) Run() {
	s.bus.Listen()
	s.scheduler.Schedule()

	for s.registry.LenEvent() > 0 {
		s.timeMutex.Lock()
		event := s.registry.PopEvent()
		*s.timeticker = event.Time + TIME_DELAY

		s.bus.AddEvent(event)
		s.timeMutex.Unlock()
	}

	log.Debug("Done")
	archive := s.registry.ArchiveTask()
	log.Infof("Task staging: %v, task running: %v, task finished: %v", len(archive[TASK_STATUS_STAGING]), len(archive[TASK_STATUS_RUNNING]), len(archive[TASK_STATUS_FINISHED]))
}
