package simulator

import (
	"os"
	. "github.com/JetMuffin/google-cluster-simulator/common"
	"path"
	"encoding/csv"
	"sort"
)

type TraceLoader struct {
	directory string
}

func NewLoader(directory string) *TraceLoader {
	return &TraceLoader{
		directory: directory,
	}
}

func (t *TraceLoader) LoadEvents() ([]*Event, int, int, int, error) {
	var totalEvents []*Event

	events, err := t.loadTaskEvents()
	if err != nil {
		return nil, 0, 0, 0, err
	}
	taskNum := len(events) - 1
	totalEvents = append(totalEvents, events...)

	events, err = t.loadJobEvents()
	if err != nil {
		return nil, 0, 0, 0, err
	}
	jobNum := len(events) - 1
	totalEvents = append(totalEvents, events...)

	events, err = t.loadMachineEvents()
	if err != nil {
		return nil, 0, 0, 0, err
	}
	machineNum := len(events) - 1
	totalEvents = append(totalEvents, events...)

	return totalEvents, machineNum, jobNum, taskNum, nil
}

func (t *TraceLoader) loadMachineEvents() ([]*Event, error) {
	file, err := os.Open(path.Join(t.directory, "machines.csv"))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var events []*Event
	for _, record := range records {
		event, err := ParseMachineEvent(record)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func (t *TraceLoader) loadJobEvents() ([]*Event, error) {
	file, err := os.Open(path.Join(t.directory, "jobs.csv"))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var events []*Event
	for _, record := range records {
		event, err := ParseJobEvent(record)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func (t *TraceLoader) loadTaskEvents() ([]*Event, error) {
	file, err := os.Open(path.Join(t.directory, "tasks.csv"))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var events []*Event
	for _, record := range records {
		event, err := ParseTaskEvent(record)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func (t *TraceLoader) LoadUsages() (map[int64][]*TaskUsage, error) {
	file, err := os.Open(path.Join(t.directory, "usages.csv"))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	m := make(map[int64][]*TaskUsage)
	usages := []TaskUsage{}
	for _, record := range records {
		usages = append(usages, ParseTaskUsage(record))
	}

	sort.Sort(UsageSort(usages))
	for _, taskUsage := range usages {
		m[GetTaskID(taskUsage.JobID, taskUsage.TaskIndex)] = append(m[GetTaskID(taskUsage.JobID, taskUsage.TaskIndex)], NewTaskUsage(taskUsage))
	}

	return m, nil
}
