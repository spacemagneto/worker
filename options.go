package worker

import (
	"runtime"
)

const DefaultMaxWorkerRestarts = 3

type config struct {
	workerCount       int
	queueSize         int
	maxWorkerRestarts int
}

func defaultConfig() config {
	defWorkerCount := runtime.NumCPU()

	return config{
		workerCount:       defWorkerCount,
		queueSize:         defWorkerCount * 2,
		maxWorkerRestarts: DefaultMaxWorkerRestarts,
	}
}
