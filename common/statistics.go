package common

import (
	log "github.com/Sirupsen/logrus"
)

type Statistics struct {
	JobNum        int64
	TaskNum       int64
	MachineNum    int64
	JobFinishTime float64
	FairIndex     float64
	Overhead      float64
	Makespan      float64
	Throughput    float64
}

func NewStatistics() *Statistics {
	return &Statistics{}
}

func (s *Statistics) Add(b *Statistics) {
	s.JobNum += b.JobNum
	s.TaskNum += b.TaskNum
	s.MachineNum += b.MachineNum
	s.FairIndex += b.FairIndex
	s.JobFinishTime += b.JobFinishTime
	s.Overhead += b.Overhead
	s.Makespan += b.Makespan
	s.Throughput += b.Throughput
}

func Report(stats []*Statistics) {
	s := NewStatistics()
	iteration := len(stats)
	for _, stat := range stats {
		s.Add(stat)
	}

	log.Infof("Job number: %v", float64(s.JobNum)/float64(iteration))
	log.Infof("Task number: %v", float64(s.TaskNum)/float64(iteration))
	log.Infof("Machine number: %v", float64(s.MachineNum)/float64(iteration))
	log.Infof("All job finished time: %v", float64(s.JobFinishTime)/float64(iteration))
	log.Infof("Jain's fair index: %v", float64(s.FairIndex)/float64(iteration))
	log.Infof("Scheduling time cost %v us", float64(s.Overhead)/float64(iteration))
	log.Infof("Average makespan: %v", float64(s.Makespan)/float64(iteration))
	log.Infof("Average throughput: %v", float64(s.Throughput)/float64(iteration))

	log.Infof("%v,%v,%v,%v,%v,%v,%v,%v", float64(s.JobNum)/float64(iteration), float64(s.TaskNum)/float64(iteration),
	float64(s.MachineNum) / float64(iteration),float64(s.JobFinishTime)/float64(iteration),float64(s.FairIndex)/float64(iteration),
		float64(s.Overhead)/float64(iteration),float64(s.Makespan)/float64(iteration),float64(s.Throughput)/float64(iteration))
}
