package monitor

import (
	. "github.com/JetMuffin/google-cluster-simulator/common"
	log "github.com/Sirupsen/logrus"
)

const (
	MONITOR_INTERVAL = 300000000
)

type Monitor struct {
	Usages map[int64][]*TaskUsage

	cpuSlack map[int64][]float64
	memSlack map[int64][]float64
	params   MonitorParam

	timeticker *int64
	interval   int64
	registry   *Registry
}

type Slack struct {
	Prediction   float64
	FirstSmooth  float64
	SecondSmooth float64
	Threshold    float64
}

type resourceSlack map[int64][]Slack

// T_{t+1} = lambda * P_{t+1} - gamma * (U_{t} - P_{t}) if U_{t} < T_{t}
//         = theta * (lambda * P_{t+1} - gamma * (U_{t} - P_{t})) else
type MonitorParam struct {
	alpha  float64
	beta   float64
	theta  float64
	lambda float64
	gamma  float64
}

func NewMonitorParam(alpha, beta, theta, lambda, gamma float64) MonitorParam {
	return MonitorParam{
		alpha:  alpha,
		beta:   beta,
		theta:  theta,
		lambda: lambda,
		gamma:  gamma,
	}
}

func NewMonitor(usages map[int64][]*TaskUsage, registry *Registry, params MonitorParam, timeticker *int64) *Monitor {
	monitor := &Monitor{
		Usages:     usages,
		cpuSlack:   make(map[int64][]float64),
		memSlack:   make(map[int64][]float64),
		interval:   MONITOR_INTERVAL,
		timeticker: timeticker,
		registry:   registry,
	}

	for taskId, taskUsages := range usages {
		var cpuUsage []float64
		var memUsage []float64
		for _, u := range taskUsages {
			cpuUsage = append(cpuUsage, u.CpuUsage)
			memUsage = append(memUsage, u.MemoryUsage)
		}
		monitor.cpuSlack[taskId] = Threshold(cpuUsage, params)
		monitor.memSlack[taskId] = Threshold(memUsage, params)
	}
	return monitor
}

func (m *Monitor) RunForever() {
	go func() {
		for {
			m.RunOnce()
		}
	}()
}

func (m *Monitor) RunOnce() (float64, float64) {
	slackCpu := 0.0
	slackMem := 0.0

	//for _, task := range m.registry.FilterTask(func(task *Task) bool { return task.Status == TASK_STATUS_RUNNING}) {
	for _, task := range m.registry.GetRunningService() {
		if m.SlackResource(task) {
			log.Debugf("Slack resource for task(%v) job(%v): cpu(%v/%v) mem(%v/%v)", task.TaskIndex, task.JobID, task.CpuSlack, task.CpuRequest, task.MemSlack, task.MemoryRequest)
		}
		if task.CpuSlack > 0 {
			slackCpu += task.CpuSlack
		}
		if task.MemSlack > 0 {
			slackMem += task.MemSlack
		}
	}

	return slackCpu, slackMem
}

func (m *Monitor) SlackResource(task *Task) bool {
	windowNum := ( *m.timeticker - task.StartTime) / m.interval
	taskId := TaskID(task)
	if windowNum == 0 || len(m.cpuSlack[taskId]) == 0 || len(m.memSlack[taskId]) == 0 || int(windowNum) > len(m.cpuSlack[taskId]) || int(windowNum) > len(m.memSlack[taskId]) {
		return false
	}

	task.CpuSlack = task.CpuRequest - m.cpuSlack[taskId][windowNum-1]
	task.MemSlack = task.MemoryRequest - m.memSlack[taskId][windowNum-1]
	m.registry.UpdateTask(task)

	if task.CpuSlack < 0 || task.MemSlack < 0 {
		return false
	}

	return true
}

func exponentialSmoothing(series []float64, alpha float64) []float64 {
	result := []float64{series[0]}
	for i := 1; i < len(series); i++ {
		result = append(result, alpha*series[i]+(1-alpha)*result[i-1])
	}
	return result
}

func doubleExponentialSmoothing(series []float64, alpha, beta float64) [] float64 {
	result := []float64{series[0]}

	var trend, level, lastLevel float64
	for i := 1; i < len(series); i++ {
		if i == 1 {
			level, trend = series[0], series[1]-series[0]
		}
		lastLevel, level = level, alpha*series[i]+(1-alpha)*(level+trend)
		trend = beta*(level-lastLevel) + (1-beta)*trend
		result = append(result, level+trend)
	}
	return result
}

func Threshold(usages []float64, params MonitorParam) []float64 {
	predicts := exponentialSmoothing(usages, params.alpha)
	thresholds := []float64{predicts[0] * params.lambda}

	for i := 1; i < len(usages); i++ {
		thresholds = append(thresholds, params.lambda*predicts[i]-params.gamma*(usages[i-1]-predicts[i-1]))
		if usages[i-1] >= thresholds[i-1] {
			thresholds[i] *= params.theta
		}
	}

	return thresholds
}
