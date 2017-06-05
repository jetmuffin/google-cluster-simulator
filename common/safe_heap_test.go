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
	for _, j := range safeJobs {
		h.PushItem(j.JobID, j, j.Share)
	}

	job := h.MinItem().Value.(Job)
	if job.Share != 0.1 {
		t.Error("Heap min item function implementation error!")
	}

	job = h.PopItem().Value.(Job)
	if job.Share != 0.1 {
		t.Logf("%+v", job)
		t.Error("Heap pop item function implementation error!")
	}

	job = h.PopItem().Value.(Job)
	if job.Share != 0.2 {
		t.Logf("%+v", job)
		t.Error("Heap pop item function implementation error!")
	}

	safeJobs[3].Share = 0.2
	h.PushItem(safeJobs[3].JobID, safeJobs[3], safeJobs[3].Share)
	job = h.MinItem().Value.(Job)
	if job.Share != 0.2 {
		t.Error("Heap min update function implementation error!")
	}
}