package monitor

import (
	. "github.com/JetMuffin/google-cluster-simulator/common"
)

const (
	MONITOR_INTERVAL = 300000000
)

type Monitor struct {
	Usages map[string][]*TaskUsage

	cpuSlack map[string][]float64
	memSlack map[string][]float64
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

func NewMonitor(usages map[string][]*TaskUsage, registry *Registry, params MonitorParam, timeticker *int64) *Monitor {
	monitor := &Monitor{
		Usages:     usages,
		cpuSlack:   make(map[string][]float64),
		memSlack:   make(map[string][]float64),
		interval:   MONITOR_INTERVAL,
		timeticker: timeticker,
		registry:   registry,
	}

	//for taskId, taskUsages := range usages {
	//	var cpuUsage []float64
	//	var memUsage []float64
	//	for _, u := range taskUsages {
	//		cpuUsage = append(cpuUsage, u.CpuUsage)
	//		memUsage = append(memUsage, u.MemoryUsage)
	//	}
	//	monitor.cpuSlack[taskId] = Threshold(cpuUsage, params)
	//	monitor.memSlack[taskId] = Threshold(memUsage, params)
	//}
	return monitor
}

func (m *Monitor) ResourceOffers() []*Machine {
	mResources := make(map[int64][]float64)

	for _, task := range m.registry.FilterTask(func(task *Task) bool { return task.Status == TASK_STATUS_RUNNING }) {
		//m.SlackResource(task)
		if m.SlackResource(task) {
			if _, ok := mResources[task.MachineID]; !ok {
				mResources[task.MachineID] = []float64{0.0, 0.0}
			}
			//log.Infof("Slack resource for task(%v) job(%v): cpu(%v/%v) mem(%v/%v)", task.TaskIndex, task.JobID, task.CpuSlack, task.CpuRequest, task.MemSlack, task.MemoryRequest)
			mResources[task.MachineID][0] += task.CpuSlack
			mResources[task.MachineID][1] += task.MemSlack
		} else {
		}
	}
	//log.Info(mResources)

	return m.registry.OversubscribedResourceOffers(mResources)
}

func (m *Monitor) SlackResource(task *Task) bool {
	windowNum := ( *m.timeticker - task.StartTime) / m.interval

	taskId := GetTaskID(task.JobID, task.TaskIndex)
	if len(m.Usages[taskId]) == 0 || int(windowNum) > len(m.Usages[taskId]) || task.StartTime == 0 {
		return false
	}
	task.CpuSlack = task.CpuRequest - m.Usages[taskId][windowNum].CpuUsage
	task.MemSlack = task.MemoryRequest - m.Usages[taskId][windowNum].MemoryUsage
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
