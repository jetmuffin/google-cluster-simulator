package base

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
