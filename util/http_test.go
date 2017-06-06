package util

import (
	"testing"
)

var (
	param = PostParam{
		Cpu: 1,
		Mem: 1024,
		AverageWaitingTime: 20,
		AverageRunningTime: 200,
		AllDoneTime: 100,
		Scheduler: 0,
	}
)

func TestPost(t *testing.T) {
	err := Post("http://localhost:5000/points", param)
	if err != nil {
		t.Error(err)
	}
}
