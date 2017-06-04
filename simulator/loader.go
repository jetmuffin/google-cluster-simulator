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
	"sort"
)

const (
	SAMPLE_TASK_PATH = "tasks.csv"
	SAMPLE_JOB_PATH = "jobs.csv"
	SAMPLE_USAGE_PATH = "task_usage.csv"
)

type TraceLoader struct {
	directory string
}

func NewLoader(directory string) *TraceLoader {
	return &TraceLoader{
		directory: directory,
	}
}

func (t *TraceLoader) LoadMarshalEvents() ([]*Event, int, error) {
	var events []*Event
	var jobNum int

	if jobEvents, err := t.LoadJobs(); err == nil {
		events = append(events, jobEvents...)
		jobNum = len(jobEvents)
	} else {
		return nil, 0, err
	}

	if taskEvents, err := t.LoadTasks(); err == nil {
		events = append(events, taskEvents...)
	} else {
		return nil, 0, err
	}

	return events, jobNum, nil
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
		log.Debugf("%+v", j)
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
		log.Debugf("%+v", t)
		events = append(events, &Event{
			Time: t.SubmitTime,
			EventOrigin: EVENT_TASK,
			TaskEventType: TASK_SUBMIT,
			Task: NewTask(t),
		})
	}

	return events, nil
}

func (t *TraceLoader) LoadUsage() (map[int64] []*TaskUsage, error) {
	f, err := os.Open(path.Join(t.directory, SAMPLE_USAGE_PATH))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	m := make(map[int64] []*TaskUsage)
	usages := []TaskUsage{}

	c, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	err = marshal.Unmarshal([]byte(c), &usages)
	if err != nil {
		return nil, err
	}

	sort.Sort(UsageSort(usages))
	for _, taskUsage := range usages {
		m[GetTaskID(taskUsage.JobID, taskUsage.TaskIndex)] = append(m[GetTaskID(taskUsage.JobID, taskUsage.TaskIndex)], NewTaskUsage(taskUsage))
	}

	return m, nil
}