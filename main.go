package main

import (
	"os"
	"fmt"
	"flag"
	log "github.com/Sirupsen/logrus"
	. "github.com/JetMuffin/google-cluster-simulator/scheduler"
	"github.com/JetMuffin/google-cluster-simulator/simulator"
)

var (
	debug bool
	directory string
	cpu float64
	mem float64
	scheduler int
)

func usage() {
	fmt.Fprintf(os.Stderr, "\nUsage: %s [flags] file [path ...]\n\n", "CommandLineFlag")
	flag.PrintDefaults()
	os.Exit(1)
}

func main() {
	flag.BoolVar(&debug, "debug", false, "Show debug logs")
	flag.StringVar(&directory, "directory", "trace", "Directory of trace data")
	flag.Float64Var(&cpu, "cpu", 10.0, "Total cpu allowed to use")
	flag.Float64Var(&mem, "mem", 1024, "Total mem allowed to use")
	flag.IntVar(&scheduler, "scheduler", 0, "Scheduler type")

	flag.Usage = usage
	flag.Parse()

	if debug {
		log.SetLevel(log.DebugLevel)
	}

	s, err := simulator.NewSimulator(directory, SchedulerType(scheduler), cpu, mem)
	if err != nil {
		log.Errorf("Cannot create simulator: %v", err)
	}

	s.Run()
}