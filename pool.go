package worker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

// TODO: rename this!
type BaseFunc func(ctx context.Context)

type Func[T any] func(context.Context, T)

type FuncError[T any] func(context.Context, T) error

type FuncWithResult[T, R any] func(context.Context, T) (R, error)

type Pool[T, R any] struct {
	ctx               context.Context
	contextCancelFunc context.CancelFunc

	jobQueue chan job[T, R]

	processingFunc FuncWithResult[T, R]

	workers int

	maxRetryWorkerRestart int32

	parentContextHook func() bool

	onceStart sync.Once

	workerWg sync.WaitGroup

	isStop atomic.Bool

	logger *slog.Logger
}

func NewFunc[T any](processingFunc Func[T], opts ...Option) (*Pool[T, struct{}], error) {
	if processingFunc == nil {
		return nil, ErrProcessingFuncIsEmpty
	}

	return NewResult(func(ctx context.Context, input T) (struct{}, error) {
		processingFunc(ctx, input)
		return struct{}{}, nil
	}, opts...)
}

func NewBaseFunc(processingFunc BaseFunc, opts ...Option) (*Pool[any, struct{}], error) {
	if processingFunc == nil {
		return nil, ErrProcessingFuncIsEmpty
	}

	return NewResult(func(ctx context.Context, _ any) (struct{}, error) {
		processingFunc(ctx)
		return struct{}{}, nil
	}, opts...)
}

func NewFuncWithError[T any](processingFunc FuncError[T], opts ...Option) (*Pool[T, struct{}], error) {
	if processingFunc == nil {
		return nil, ErrProcessingFuncIsEmpty
	}

	return NewResult(func(ctx context.Context, input T) (struct{}, error) {
		return struct{}{}, processingFunc(ctx, input)
	}, opts...)
}

func NewResult[T, R any](processingFunc FuncWithResult[T, R], opts ...Option) (*Pool[T, R], error) {
	if processingFunc == nil {
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
		processingFunc:        processingFunc,
		maxRetryWorkerRestart: int32(cfg.maxWorkerRestarts),
		logger:                cfg.logger,
	}

	// I stole the idea after seeing a solution on GitHub for cleaning up resources once the main context had finished.
	pool.parentContextHook = context.AfterFunc(ctx, func() {
		pool.Stop()
	})

	return pool, nil
}

func (p *Pool[T, R]) Run() {
	p.onceStart.Do(func() {
		for index := range p.workers {
			wr := newWorker[T, R](p.ctx, index, p.processingFunc, p.jobQueue, p.maxRetryWorkerRestart, p.logger)

			p.workerWg.Add(1)
			go wr.start(&p.workerWg)
		}
	})
}

func (p *Pool[T, R]) AddJob(data T, opts ...JobOptions) error {
	return p.submit(data, nil, opts...)
}

func (p *Pool[T, R]) AddJobWithResult(data T, opts ...JobOptions) (*ProcessingResult[R], error) {
	res := newProcessingResult[R]()
	if err := p.submit(data, res, opts...); err != nil {
		return nil, err
	}

	return res, nil
}

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

func (p *Pool[T, R]) Stop() {
	if !p.isStop.Swap(true) {
		if p.parentContextHook != nil {
			p.parentContextHook()
		}

		p.contextCancelFunc()
	}

	p.workerWg.Wait()
}
