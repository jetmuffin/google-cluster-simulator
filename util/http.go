package util

import (
	"net/http"
	"net/url"
	"fmt"
	"bytes"
	"time"
)

type PostParam struct {
	Cpu                float64
	Mem                float64
	AverageRunningTime float64
	AverageWaitingTime float64
	AllDoneTime        float64
	Scheduler          int64
}

func Post(apiUrl string, param PostParam) error {
	form := url.Values{
		"cpu":                {fmt.Sprintf("%.6f", param.Cpu)},
		"mem":                {fmt.Sprintf("%.6f", param.Mem)},
		"avg_wait_time":      {fmt.Sprintf("%.6f", param.AverageWaitingTime)},
		"avg_running_time":   {fmt.Sprintf("%.6f", param.AverageRunningTime)},
		"total_running_time": {fmt.Sprintf("%.6f", param.AllDoneTime)},
		"scheduler":          {fmt.Sprintf("%d", param.Scheduler)},
		"timestamp":          {fmt.Sprintf("%d", time.Now().Unix())},
	}

	body := bytes.NewBufferString(form.Encode())
	_, err := http.Post(apiUrl, "application/x-www-form-urlencoded", body)
	return err
}
