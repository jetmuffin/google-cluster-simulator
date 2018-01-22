package common

import (
	"container/heap"
	"sync"
	log "github.com/Sirupsen/logrus"
)

type JobHeap []*Job

func NewJobHeap() JobHeap {
	var jobs []*Job
	h := JobHeap(jobs)
	heap.Init(&h)
	return h
}

func (h JobHeap) Len() int {
	return len(h)
}

func (h JobHeap) Less(i, j int) bool {
	if h[i].Share == h[j].Share {
		if len(h[i].taskQueue) == len(h[j].taskQueue) {
			return h[i].SubmitTime < h[j].SubmitTime
		}
		return len(h[i].taskQueue) > len(h[j].taskQueue)
	}
	return h[i].Share < h[j].Share
}

func (h JobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *JobHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*Job)
	item.index = n
	*h = append(*h, item)
}

func (h *JobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	item.index = -1
	*h = old[0: n-1]
	return item
}

type JobSyncHeap struct {
	heap *JobHeap
	lock sync.RWMutex
}

func NewJobSyncHeap() *JobSyncHeap {
	h := NewJobHeap()
	return &JobSyncHeap{
		heap: &h,
	}
}

func (h *JobSyncHeap) PushJob(job *Job) {
	h.lock.Lock()
	defer h.lock.Unlock()

	heap.Push(h.heap, job)
}

func (h *JobSyncHeap) PopJob() *Job {
	h.lock.Lock()
	defer h.lock.Unlock()

	return heap.Pop(h.heap).(*Job)
}

func (h *JobSyncHeap) TopJob() *Job {
	h.lock.RLock()
	defer h.lock.RUnlock()

	if len(*h.heap) == 0 {
		return nil
	}
	l := *h.heap
	return l[0]
}

func (h *JobSyncHeap) UpdateJob(job *Job) {
	h.lock.Lock()
	defer h.lock.Unlock()

	heap.Fix(h.heap, job.index)
}

func (h *JobSyncHeap) Debug() {
	h.lock.RLock()
	defer h.lock.RUnlock()

	for _, v := range *h.heap {
		log.Debugf("%v %v %v [%v]",v.JobID, v.Share, len(v.taskQueue), v.SubmitTime/1000/1000)
	}

	log.Debug()
}

type EventHeap []*Event

func NewEventHeap(events []*Event) EventHeap {
	h := EventHeap(events)
	heap.Init(&h)
	return h
}

func (h EventHeap) Len() int {
	return len(h)
}

func (h EventHeap) Less(i, j int) bool {
	return h[i].Time < h[j].Time
}

func (h EventHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *EventHeap) Push(x interface{}) {
	item := x.(*Event)
	*h = append(*h, item)
}

func (h *EventHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0: n-1]
	return item
}
