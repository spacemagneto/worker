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

// WithWorkers sets how many goroutines will be processing jobs concurrently.
// Defaults to runtime.NumCPU() - a reasonable starting point for CPU-bound work,
// but you'll want more for IO-bound workloads like HTTP calls or DB queries.
func WithWorkers(workers int) Option {
	return func(cfg *config) {
		cfg.workers = workers
	}
}

// WithQueueSize sets the capacity of the job channel.
// Defaults to runtime.NumCPU() * 2. This defines how many jobs can be queued
// before AddJob starts blocking (backpressure). Adjust based on memory
// availability and throughput requirements.
func WithQueueSize(size int) Option {
	return func(cfg *config) {
		cfg.queueSize = size
	}
}

// WithMaxRestarts sets the retry limit for a worker after a panic occurs.
// Defaults to 3. If a worker hits this limit, it will terminate permanently to prevent endless panic loops.
// NOTE: Don’t forget that in computer science, the problem always lies somewhere between the chair and the keyboard,
// and sometimes it’s not the library’s author fault.
func WithMaxRestarts(maxRestarts int) Option {
	return func(cfg *config) {
		cfg.maxWorkerRestarts = maxRestarts
	}
}

// WithContext ties the pool's lifetime to an external context.
// When that context is cancelled, the pool stops automatically no need to
// call Stop() manually. Useful for hooking into application shutdown signals.
func WithContext(ctx context.Context) Option {
	return func(cfg *config) {
		cfg.context = ctx
	}
}

// WithLogger lets you plug in your own slog.Logger.
// By default, the pool logs to stdout in JSON, which is fine for local dev,
// but you probably want something structured with your app's log level in prod.
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

// WithTimeout sets a deadline for a single jobs execution.
// When it fires, context passed to your Processor is cancelled
// so your handler needs to respect ctx.Done() for this to actually work.
func WithTimeout(timeout time.Duration) JobOptions {
	return func(options *jobOptions) {
		options.timeout = timeout
	}
}

// WithErrorFunc registers a callback that runs if the job returns an error or panic.
// It's called synchronously inside the worker after the job finishes,
// so keep it lightweight logging, metrics, maybe a notification, not another heavy operation.
func WithErrorFunc(fn func(err error)) JobOptions {
	return func(options *jobOptions) {
		options.errorFunc = fn
	}
}
