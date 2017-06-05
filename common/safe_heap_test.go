package common

import (
	"testing"
)

var (
	safeJobs = []Job{
		{
			JobID: 1,
			Share: 0.2,
		},
		{
			JobID: 2,
			Share: 0.1,
		},
		{
			JobID: 3,
			Share: 0.5,
		},
		{
			JobID: 4,
			Share: 0.3,
		},
		{
			JobID: 5,
			Share: 0.6,
		},
		{
			JobID: 6,
			Share: 0.3,
		},
	}
	h PriorityQueue
)

func TestSafeHeap(t *testing.T) {
	h.Init(1024)
	var jobs []*Job
	for _, j := range safeJobs {
		jb := NewJob(j)
		jb.taskQueue.PushTask(&Task{})
		jobs = append(jobs, jb)
	}

	for _, j := range jobs {
		h.PushItem(j.JobID, j)
	}

	job := h.MinItem()
	if job.Share != 0.1 {
		t.Error("Heap min item function implementation error!")
	}

	job = h.PopItem()
	if job.Share != 0.1 {
		t.Logf("%+v", job)
		t.Error("Heap pop item function implementation error!")
	}

	job = h.PopItem()
	if job.Share != 0.2 {
		t.Logf("%+v", job)
		t.Error("Heap pop item function implementation error!")
	}

	jobs[3].Share = 0.2
	h.UpdateItem(jobs[3].JobID, jobs[3])
	job = h.MinItem()
	if job.Share != 0.2 {
		t.Logf("%+v", job)
		outJob := h.GetQueueItems()
		for _, j := range outJob {
			t.Logf("%+v", j)
		}
		t.Error("Heap min update function implementation error!")
	}
}