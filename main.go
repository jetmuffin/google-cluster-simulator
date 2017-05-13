package main

import (
	"os"
	"fmt"
	"flag"
	log "github.com/Sirupsen/logrus"
	"github.com/JetMuffin/google-cluster-simulator/simulator"
	"github.com/JetMuffin/google-cluster-simulator/scheduler"
)

var (
	debug bool
	directory string
)

func usage() {
	fmt.Fprintf(os.Stderr, "\nUsage: %s [flags] file [path ...]\n\n", "CommandLineFlag")
	flag.PrintDefaults()
	os.Exit(1)
}

func main() {
	flag.BoolVar(&debug, "debug", false, "Show debug logs")
	flag.StringVar(&directory, "directory", "trace", "Directory of trace data")

	flag.Usage = usage
	flag.Parse()

	if(debug) {
		log.SetLevel(log.DebugLevel)
	}

	//f, err := os.OpenFile("out.log", os.O_WRONLY | os.O_CREATE, 0755)
	//log.SetOutput(f)

	s, err := simulator.NewSimulator(directory, scheduler.SCHEDULER_DRF)
	if err != nil {
		log.Errorf("Cannot create simulator: %v", err)
	}

	s.Run()
}