package worker

import (
	"context"
	"runtime"
)

const DefaultMaxWorkerRestarts = 3

type Option func(*config)

type config struct {
	workers           int
	queueSize         int
	maxWorkerRestarts int
	context           context.Context
}

func defaultConfig() config {
	defWorkerCount := runtime.NumCPU()

	return config{
		workers:           defWorkerCount,
		queueSize:         defWorkerCount * 2,
		maxWorkerRestarts: DefaultMaxWorkerRestarts,
		context:           context.Background(),
	}
}

func WithWorkers(n int) Option {
	return func(cfg *config) {
		cfg.workers = n
	}
}

func WithQueueSize(n int) Option {
	return func(cfg *config) {
		cfg.queueSize = n
	}
}

func WithMaxWorkerRestarts(n int) Option {
	return func(cfg *config) {
		cfg.maxWorkerRestarts = n
	}
}

func WithContext(ctx context.Context) Option {
	return func(cfg *config) {
		cfg.context = ctx
	}
}
