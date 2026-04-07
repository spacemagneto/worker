package worker

import (
	"context"
	"runtime"
	"time"
)

const DefaultMaxWorkerRestarts = 3

type config struct {
	context           context.Context
	workers           int
	queueSize         int
	maxWorkerRestarts int
}

type Option func(*config)

func defaultConfig() config {
	defaultWorkerCount := runtime.NumCPU()

	return config{
		context:           context.Background(),
		workers:           defaultWorkerCount,
		queueSize:         defaultWorkerCount * 2,
		maxWorkerRestarts: DefaultMaxWorkerRestarts,
	}
}

func WithWorkers(workers int) Option {
	return func(cfg *config) {
		cfg.workers = workers
	}
}

func WithQueueSize(size int) Option {
	return func(cfg *config) {
		cfg.queueSize = size
	}
}

func WithMaxWorkerRestarts(maxRestarts int) Option {
	return func(cfg *config) {
		cfg.maxWorkerRestarts = maxRestarts
	}
}

func WithContext(ctx context.Context) Option {
	return func(cfg *config) {
		cfg.context = ctx
	}
}

type jobOptions struct {
	timeout   time.Duration
	errorFunc func(error)
}

type JobOptions func(*jobOptions)

func WithTimeout(timeout time.Duration) JobOptions {
	return func(options *jobOptions) {
		options.timeout = timeout
	}
}

func WithErrorFunc(fn func(err error)) JobOptions {
	return func(options *jobOptions) {
		options.errorFunc = fn
	}
}
