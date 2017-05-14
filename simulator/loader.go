package simulator

import (
	"os"
	. "github.com/JetMuffin/google-cluster-simulator/base"
	"github.com/JetMuffin/google-cluster-simulator/marshal"
	log "github.com/Sirupsen/logrus"
	"path"
	"bufio"
	"io"
	"io/ioutil"
)

const (
	MACHINE_EVENT_PATH = "machine_events/part-00000-of-00001.csv"
	JOB_EVENT_PATH     = "job_events/part-00000-of-00500.csv"
	TASK_EVENT_PATH    = "task_events/part-00000-of-00500.csv"

	SAMPLE_TASK_PATH = "sample/tasks.csv"
	SAMPLE_JOB_PATH = "sample/jobs.csv"
)

type TraceLoader struct {
	directory string
}

func NewLoader(directory string) *TraceLoader {
	return &TraceLoader{
		directory: directory,
	}
}

func (t *TraceLoader) LoadEvents() ([]*Event, error) {
	var events []*Event

	machineEvents, err := t.LoadMachineEvents()
	if err == nil {
		events = append(events, machineEvents...)
	} else {
		log.Error(err)
		return nil, err
	}

	jobEvents, err := t.LoadJobEvents()
	if err == nil {
		events = append(events, jobEvents...)
	} else {
		return nil, err
	}

	taskEvents, err := t.LoadTaskEvents()
	if err == nil {
		events = append(events, taskEvents...)
	} else {
		return nil, err
	}

	return events, nil
}

func (t *TraceLoader) LoadMachineEvents() ([]*Event, error) {
	f, err := os.Open(path.Join(t.directory, MACHINE_EVENT_PATH))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var events []*Event
	rd := bufio.NewReader(f)

	for {
		line, err := rd.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}

		event, err := ParseMachineEvent(line)
		if err != nil {
			continue
		}
		events = append(events, event)
	}
	return events, nil
}

func (t *TraceLoader) LoadJobEvents() ([]*Event, error) {
	f, err := os.Open(path.Join(t.directory, JOB_EVENT_PATH))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var events []*Event
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}

		event, err := ParseJobEvent(line)
		if err != nil || event.Time == 0 {
			continue
		}
		events = append(events, event)
	}
	return events, nil
}

func (t *TraceLoader) LoadTaskEvents() ([]*Event, error) {
	f, err := os.Open(path.Join(t.directory, TASK_EVENT_PATH))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var events []*Event
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}

		event, err := ParseTaskEvent(line)
		if err != nil || event.Time == 0{
			continue
		}
		events = append(events, event)
	}
	return events, nil
}

func (t *TraceLoader) LoadMarshalEvents() ([]*Event, error) {
	var events []*Event

	if jobEvents, err := t.LoadJobs(); err == nil {
		events = append(events, jobEvents...)
	} else {
		return nil, err
	}

	if taskEvents, err := t.LoadTasks(); err == nil {
		events = append(events, taskEvents...)
	} else {
		return nil, err
	}

	return events, nil
}

func (t *TraceLoader) LoadJobs() ([]*Event, error) {
	f, err := os.Open(path.Join(t.directory, SAMPLE_JOB_PATH))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var events []*Event
	jobs := []Job{}

	c, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	err = marshal.Unmarshal([]byte(c), &jobs)
	if err != nil {
		return nil, err
	}

	for _, j := range jobs {
		events = append(events, &Event{
			Time: j.SubmitTime,
			EventOrigin: EVENT_JOB,
			TaskEventType: TASK_SUBMIT,
			Job: NewJob(j),
		})
	}

	return events, nil
}

func (t *TraceLoader) LoadTasks() ([]*Event, error) {
	f, err := os.Open(path.Join(t.directory, SAMPLE_TASK_PATH))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var events []*Event
	tasks := []Task{}

	c, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	err = marshal.Unmarshal([]byte(c), &tasks)
	if err != nil {
		return nil, err
	}

	for _, t := range tasks {

		events = append(events, &Event{
			Time: t.SubmitTime,
			EventOrigin: EVENT_TASK,
			TaskEventType: TASK_SUBMIT,
			Task: NewTask(t),
		})
	}

	return events, nil
}