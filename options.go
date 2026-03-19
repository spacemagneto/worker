package worker

import (
	"runtime"
)

const DefaultMaxWorkerRestarts = 3

type Option func(*config)

type config struct {
	workers           int
	queueSize         int
	maxWorkerRestarts int
}

func defaultConfig() config {
	defWorkerCount := runtime.NumCPU()

	return config{
		workers:           defWorkerCount,
		queueSize:         defWorkerCount * 2,
		maxWorkerRestarts: DefaultMaxWorkerRestarts,
	}
}

func WithWorkers(n int) Option {
	return func(cfg *config) {
		cfg.workers = n
	}
}
