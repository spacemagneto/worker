package worker

import (
	"context"
	"sync"
	"sync/atomic"
)

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

func (p *Pool[T, R]) AddJob(data T) error {
	return nil
}

func (p *Pool[T, R]) AddJobWithResult(data T) (*ProcessingResult[R], error) {
	return nil, nil
}

func (p *Pool[T, R]) submit(data T, res *ProcessingResult[R]) error {
	if p.isStop.Load() {
		return ErrPoolStopped
	}

	select {
	// TODO: add job options for timeout and error handler
	case p.jobQueue <- job[T, R]{data: data, jobResult: res}:
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
