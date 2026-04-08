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

func (p *Pool[T, R]) Run() {
	p.onceStart.Do(func() {
		for index := range p.workers {
			wr := newWorker[T, R](p.ctx, index, p.processingFunc, p.jobQueue, p.maxRetryWorkerRestart)

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
