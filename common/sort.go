package common

type JobShareSort []*Job

func (s JobShareSort) Len() int {
	return len(s)
}

func (s JobShareSort) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s JobShareSort) Less(i, j int) bool {
	if s[i] == nil || s[j] == nil {
		return true
	}
	if s[i].Share == s[j].Share {
		if len(s[i].taskQueue) == len(s[j].taskQueue) {
			return s[i].SubmitTime < s[j].SubmitTime
		}
		return len(s[i].taskQueue) > len(s[j].taskQueue)
	}
	return s[i].Share < s[j].Share
}

type UsageSort []TaskUsage

func (u UsageSort) Len() int {
	return len(u)
}

func (u UsageSort) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}

func (u UsageSort) Less(i, j int) bool {
	return u[i].StartTime < u[j].StartTime
}

type TaskSort []*Task

func (t TaskSort) Len() int {
	return len(t)
}

func (t TaskSort) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t TaskSort) Less(i, j int) bool {
	return t[i].SubmitTime < t[j].SubmitTime
}