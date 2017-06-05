package common

import (
	"testing"
	"sort"
	"reflect"
)

func TestSort(t *testing.T) {
	jobs := make([]*Job, 0, 10)

	job := NewJob(Job {
		Share: 0.1,
		JobID: 1,
	})
	jobs = append(jobs, job)

	sort.Sort(JobShareSort(jobs))

	if !reflect.DeepEqual(jobs[0], job) {
		t.Error("sort by share error")
	}
}