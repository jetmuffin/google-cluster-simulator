package base

import (
	"testing"
)

var (
	jobs = []*Job{
		{
			JobID: 1,
			Share: 0.2,
		},
		{
			JobID:2,
			Share:0.1,
		},
		{
			JobID:3,
			Share:0.5,
		},
		{
			JobID:4,
			Share:0.3,
		},
		{
			JobID:5,
			Share:0.6,
		},
	}
)

func TestJobHeap(t *testing.T) {
	h := NewJobHeap()
	for _, v := range jobs {
		h.PushJob(v)
	}

	job := h.PopJob()
	t.Log(job.JobID, job.Share)
	if job.Share != 0.1 {
		t.Error("Heap implementation error")
	}

	job = h.TopJob()
	if job.Share != 0.2 {
		t.Error("Heap implementation error")
	}

	h.UpdateShare(job, 0.8)
	job = h.TopJob()
	if job.Share != 0.3 {
		t.Error("Heap implementation error")
	}
}


