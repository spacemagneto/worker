package worker

import (
	"context"
	"log/slog"
	"os"
	"runtime"
	"time"
)

const DefaultMaxWorkerRestarts = 3

type config struct {
	context           context.Context
	workers           int
	queueSize         int
	maxWorkerRestarts int
	logger            *slog.Logger
}

type Option func(*config)

func defaultConfig() config {
	defaultWorkerCount := runtime.NumCPU()

	return config{
		context:           context.Background(),
		workers:           defaultWorkerCount,
		queueSize:         defaultWorkerCount * 2,
		maxWorkerRestarts: DefaultMaxWorkerRestarts,
		// TODO: Actually, what about the options? Should we leave them as they are or add some new ones?
		logger: slog.New(slog.NewJSONHandler(os.Stdout, nil)),
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

func WithMaxRestarts(maxRestarts int) Option {
	return func(cfg *config) {
		cfg.maxWorkerRestarts = maxRestarts
	}
}

func WithContext(ctx context.Context) Option {
	return func(cfg *config) {
		cfg.context = ctx
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(c *config) {
		c.logger = logger
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
