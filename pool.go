package worker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

// Task represents a simple unit of work that requires only a context.
// Use this for fire-and-forget background operations.
type Task func(ctx context.Context)

// Handler represents a one-way data processing function.
// It accepts input of type T but does not return a result or error.
type Handler[T any] func(context.Context, T)

// ErrHandler is like Handler but lets the worker know something went wrong.
// Useful when you want per-job error callbacks via WithErrorFunc.
type ErrHandler[T any] func(context.Context, T) error

// Processor is the full version - input in, result out, error if needed.
// Everything else (NewTaskPool, NewPool, NewErrorPool) wraps this underneath.
type Processor[T, R any] func(context.Context, T) (R, error)

type Pool[T, R any] struct {
	ctx               context.Context
	contextCancelFunc context.CancelFunc

	jobQueue chan job[T, R]

	processingFunc Processor[T, R]

	workers int

	maxRetryWorkerRestart int32

	parentContextHook func() bool

	onceStart sync.Once

	workerWg sync.WaitGroup

	isStop atomic.Bool

	logger *slog.Logger
}

// NewTaskPool creates a pool for executing simple standalone tasks.
// Ideal for periodic background jobs that don't depend on external data.
func NewTaskPool(handler Task, opts ...Option) (*Pool[any, struct{}], error) {
	if handler == nil {
		return nil, ErrProcessingFuncIsEmpty
	}

	return New(func(ctx context.Context, _ any) (struct{}, error) {
		handler(ctx)
		return struct{}{}, nil
	}, opts...)
}

// NewPool creates a pool where each job gets a piece of data to work with.
// The simplest constructor when you don't need to track errors or return results
// just do it - cosplay Shia LaBeouf.
func NewPool[T any](handler Handler[T], opts ...Option) (*Pool[T, struct{}], error) {
	if handler == nil {
		return nil, ErrProcessingFuncIsEmpty
	}

	return New(func(ctx context.Context, input T) (struct{}, error) {
		handler(ctx, input)
		return struct{}{}, nil
	}, opts...)
}

// NewErrorPool is the middle ground: you want to know when a job fails,
// but you still don't need a return value. Errors will flow into the
// WithErrorFunc callback if you set one on the job.
func NewErrorPool[T any](handler ErrHandler[T], opts ...Option) (*Pool[T, struct{}], error) {
	if handler == nil {
		return nil, ErrProcessingFuncIsEmpty
	}

	return New(func(ctx context.Context, input T) (struct{}, error) {
		return struct{}{}, handler(ctx, input)
	}, opts...)
}

// New is the base constructor that everyone else delegates to.
// Use this directly when you need to get a result back from each job -
// the returned *ProcessingResult lets you wait for it or check if it's done.
func New[T, R any](handler Processor[T, R], opts ...Option) (*Pool[T, R], error) {
	if handler == nil {
		return nil, ErrProcessingFuncIsEmpty
	}

	cfg := defaultConfig()

	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	ctx, cancel := context.WithCancel(cfg.context)

	pool := &Pool[T, R]{
		ctx:                   ctx,
		contextCancelFunc:     cancel,
		workers:               cfg.workers,
		jobQueue:              make(chan job[T, R], cfg.queueSize),
		processingFunc:        handler,
		maxRetryWorkerRestart: int32(cfg.maxWorkerRestarts),
		logger:                cfg.logger,
	}

	// I stole the idea after seeing a solution on GitHub for cleaning up resources once the main context had finished.
	pool.parentContextHook = context.AfterFunc(ctx, func() {
		pool.Stop()
	})

	return pool, nil
}

// Run starts the worker goroutines. Call it once after creating the pool
// the sync.Once inside makes sure you can't accidentally start it twice.
// Workers won't do anything until this is called.
//
// NOTE: Spoiler, even after calling Run(), workers will wait for you to add data
// before they actually execute any handlers. Don't assume that just because
// you added a handler to the constructor, everything starts working instantly.
// No, first you add the data, and then we'll kick the handler (oh, excuse me, launch it).
func (p *Pool[T, R]) Run() {
	p.onceStart.Do(func() {
		for index := range p.workers {
			wr := newWorker[T, R](p.ctx, index, p.processingFunc, p.jobQueue, p.maxRetryWorkerRestart, p.logger)

			p.workerWg.Add(1)
			go wr.start(&p.workerWg)
		}
	})
}

// AddJob queues a job for processing without tracking its result.
// Blocks if the queue is full until either a slot opens or the context is done.
// Returns ErrPoolStopped if Stop() was already called.
func (p *Pool[T, R]) AddJob(data T, opts ...JobOptions) error {
	return p.submit(data, nil, opts...)
}

// AddJobWithResult queues a job and gives you a handle to track its outcome.
// The returned *ProcessingResult lets you either wait for the result or
// check if it's done without blocking useful when you need to do other
// work while the job runs.
// Returns ErrPoolStopped if Stop() was already called.
func (p *Pool[T, R]) AddJobWithResult(data T, opts ...JobOptions) (*ProcessingResult[R], error) {
	res := newProcessingResult[R]()
	if err := p.submit(data, res, opts...); err != nil {
		return nil, err
	}

	return res, nil
}

// submit is the internal path that both AddJob and AddJobWithResult go through.
// Keeping the result pointer optional here lets us avoid branching at the call sites.
func (p *Pool[T, R]) submit(data T, res *ProcessingResult[R], opts ...JobOptions) error {
	if p.isStop.Load() {
		p.logger.Error("pool is close")
		return ErrPoolStopped
	}

	var options jobOptions

	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}

	select {
	case p.jobQueue <- job[T, R]{data: data, jobResult: res, errorFunc: options.errorFunc, timeout: options.timeout}:
		return nil

	case <-p.ctx.Done():
		p.logger.Error("context is done", "error", p.ctx.Err())
		return p.ctx.Err()
	}
}

// Stop shuts the pool down and waits for all workers to finish.
// It's safe to call multiple times - the atomic swap ensures only the first
// call does the actual work. Any jobs still in the queue will be drained
// by workers before they exit, so pending ProcessingResult channels won't leak.
func (p *Pool[T, R]) Stop() {
	if !p.isStop.Swap(true) {
		if p.parentContextHook != nil {
			p.parentContextHook()
		}

		p.contextCancelFunc()
	}

	p.workerWg.Wait()
}
