package common

import (
	"testing"
)

var (
	jobs = []Job{
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
)

func TestJobHeap(t *testing.T) {
	h := NewJobSyncHeap()
	for _, v := range jobs {
		h.PushJob(NewJob(v))
	}

	job := h.PopJob()
	t.Log(job.JobID, job.Share)
	if job.Share != 0.1 {
		t.Error("Heap implementation error")
	}

	job = h.TopJob()
	t.Log(job.JobID, job.Share)
	if job.Share != 0.2 {
		t.Error("Heap implementation error")
	}
	job.Share = 0.9
	h.UpdateJob(job)

	job = h.TopJob()
	t.Log(job.JobID, job.Share, job.taskQueue.Len())
	if job.Share != 0.3 {
		t.Error("Heap implementation error")
	}

	for _, v := range *h.heap {
		if v.JobID == 6 {
			v.taskQueue.PushTask(&Task{})
		}
	}
	job = h.TopJob()
	t.Log(job.JobID, job.Share, job.taskQueue.Len())
	if job.Share != 0.3 {
		t.Error("Heap does not sort by job queue len")
	}
}
