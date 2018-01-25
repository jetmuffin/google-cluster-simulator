package main

import (
	"os"
	"fmt"
	"flag"
	log "github.com/Sirupsen/logrus"
	. "github.com/JetMuffin/google-cluster-simulator/common"
	"github.com/JetMuffin/google-cluster-simulator/simulator"
	"runtime"
)

var (
	config Config
	cores int
)

func usage() {
	fmt.Fprintf(os.Stderr, "\nUsage: %s [flags] file [path ...]\n\n", "CommandLineFlag")
	flag.PrintDefaults()
	os.Exit(1)
}

func main() {
	flag.BoolVar(&config.Debug, "debug", false, "Show debug logs")
	flag.BoolVar(&config.Post, "post", false, "Post result to flask")
	flag.StringVar(&config.Directory, "directory", "trace", "Directory of trace data")
	flag.Float64Var(&config.Cpu, "cpu", 10.0, "Total cpu allowed to use")
	flag.Float64Var(&config.Mem, "mem", 1024, "Total mem allowed to use")
	flag.IntVar(&config.Scheduler, "scheduler", 0, "Scheduler type")
	flag.Float64Var(&config.Alpha, "alpha", 0.5, "single exponential influence")
	flag.Float64Var(&config.Beta, "beta", 0.3, "double exponential influence")
	flag.Float64Var(&config.Theta, "theta", 1.2, "punish parameter")
	flag.Float64Var(&config.Lambda, "lambda", 1.0, "threshold parameter")
	flag.Float64Var(&config.Gamma, "gamma", 0.1, "predictor error feedback")
	flag.IntVar(&config.Iteration, "iter", 1, "Iteration")
	flag.IntVar(&cores, "cores", 1, "Number of cores")

	flag.Usage = usage
	flag.Parse()

	//f, err := os.OpenFile("out.log", os.O_WRONLY | os.O_CREATE, 0755)
	//log.SetOutput(f)

	if config.Debug {
		log.SetLevel(log.DebugLevel)
	}

	if cores > 1 {
		runtime.GOMAXPROCS(cores)
	}

	var stats []*Statistics
	for i := 1; i <= config.Iteration; i++ {
		log.Infof("Iteration %v:", i)

		s, err := simulator.NewSimulator(config)
		if err != nil {
			log.Errorf("Cannot create simulator: %v", err)
		}

		stats = append(stats, s.Run())
	}

	log.Info("=========================================")
	if (config.Scheduler) == 0 {
		log.Info("Scheduler: DRF")
	} else {
		log.Info("Scheduler: Datom")
	}
	Report(stats)
	log.Info("=========================================")
}
