package common

import (
	"testing"
	"reflect"
)

var (
	taskLine = "0,,3418314,0,3938719206,0,70s3v5qRyCO/1PCdI6fVXnrW8FU/w+5CKRSa72xgcIo=,3,9,0.125,0.07446,0.0004244,0"
	expectTask = &Task{
		MissingInfo: 0,
		JobID: 3418314,
		TaskIndex: 0,
		MachineID: 3938719206,
		User: "70s3v5qRyCO/1PCdI6fVXnrW8FU/w+5CKRSa72xgcIo=",
		SchedulingClass: 3,
		Priority: 9,
		CpuRequest: 0.125,
		MemoryRequest: 0.07446,
		DiskSpaceRequest: 0.0004244,
		DifferentMachinesRestriction: false,
	}

	machineLine = "0,5,0,HofLGzk1Or/8Ildj2+Lqv0UGGvY82NLoni8+J/Yy0RU=,0.5,0.2493"
	expectMachine = &Machine{
		MachineID: 5,
		PlatformID: "HofLGzk1Or/8Ildj2+Lqv0UGGvY82NLoni8+J/Yy0RU=",
		Cpus: 0.5,
		Mem: 0.2493,
	}

	jobLine = "0,,3418309,0,70s3v5qRyCO/1PCdI6fVXnrW8FU/w+5CKRSa72xgcIo=,3,IHgtoxEBuUTHNbUeVs4hzptMY4n8rZKLbZg+Jh5fNG4=,wAmgn2H74cdoMuSFwJF3NaUEaudVBTZ0/HaNZBwIpEQ="
	expectJob = &Job {
		JobID: 3418309,
		User: "70s3v5qRyCO/1PCdI6fVXnrW8FU/w+5CKRSa72xgcIo=",
		SchedulingClass: 3,
		JobName: "IHgtoxEBuUTHNbUeVs4hzptMY4n8rZKLbZg+Jh5fNG4=",
		LogicalJobName: "wAmgn2H74cdoMuSFwJF3NaUEaudVBTZ0/HaNZBwIpEQ=",
	}
)

func TestParseTaskEvent(t *testing.T) {
	taskEvent, err := ParseTaskEvent(taskLine)
	if err != nil {
		t.Errorf("Parse task line error: %v", err)
	}
	if !reflect.DeepEqual(taskEvent.Task, expectTask) {
		t.Error("Parse task error")
	}
}

func TestParseMachineEvent(t *testing.T) {
	machineEvent, err := ParseMachineEvent(machineLine)
	if err != nil {
		t.Errorf("Parse machine line error: %v", err)
	}
	if !reflect.DeepEqual(machineEvent.Machine, expectMachine) {
		t.Error("Parse machine error")
	}
}

func TestParseJobEvent(t *testing.T) {
	jobEvent, err := ParseJobEvent(jobLine)
	if err != nil {
		t.Errorf("Parse job line error: %v", err)
	}
	if !reflect.DeepEqual(jobEvent.Job, expectJob) {
		t.Error("Parse job error")
	}
}