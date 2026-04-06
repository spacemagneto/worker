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

func (p *Pool[T, R]) Run() {}

func (p *Pool[T, R]) AddJob(data T) error {
	return nil
}

func (p *Pool[T, R]) AddJobWithResult(data T) (*ProcessingResult[R], error) {
	return nil, nil
}

func (p *Pool[T, R]) Stop() {}
