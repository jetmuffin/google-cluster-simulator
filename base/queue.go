package base

import "sync"

type TaskQueue []*Task

func (q *TaskQueue) Push(task *Task) {
	*q = append(*q, task)
}

func (q *TaskQueue) Pop() (task *Task) {
	task = (*q)[0]
	*q = (*q)[1:]
	return task
}

func (q *TaskQueue) Len() int {
	return len(*q)
}

type SyncTaskQueue struct {
	queue *TaskQueue
	lock  sync.RWMutex
}

func NewSyncTaskQueue() *SyncTaskQueue {
	return &SyncTaskQueue{
		queue: new(TaskQueue),
	}
}

func (q *SyncTaskQueue) PushTask(task *Task) {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.queue.Push(task)
}

func (q *SyncTaskQueue) PopTask() *Task {
	q.lock.Lock()
	defer q.lock.Unlock()

	return q.queue.Pop()
}

func (q *SyncTaskQueue) Len() int {
	q.lock.RLock()
	defer q.lock.RUnlock()

	return q.queue.Len()
}

func (q *SyncTaskQueue) Peek() *Task {
	q.lock.RLock()
	defer q.lock.RUnlock()

	if q.queue.Len() == 0 {
		return nil
	}
	return (*q.queue)[0]
}
