package base

import "container/heap"

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

func (h *JobHeap) PushJob(job *Job) {
	heap.Push(h, job)
}

func (h *JobHeap) PopJob() *Job {
	return heap.Pop(h).(*Job)
}

func (h *JobHeap) TopJob() *Job {
	if len(*h) == 0 {
		return nil
	}
	l := *h
	return l[0]
}

func (h *JobHeap) TopIndexJob(index int) *Job {
	if index >= len(*h) {
		return nil
	}
	l := *h
	return l[index]
}

func (h *JobHeap) UpdateShare(job *Job, share float64) {
	job.Share = share
	heap.Fix(h, job.index)
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


