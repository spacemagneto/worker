package worker

import (
	"context"
	"fmt"
	"sync/atomic"
)

type worker[T, R any] struct {
	parentCtx             context.Context
	id                    string
	processingFunc        FuncWithResult[T, R]
	jobCh                 <-chan job[T, R]
	maxRetryWorkerRestart int32
	restartCounter        atomic.Int32
}

func newWorker[T, R any](ctx context.Context, index int, processingFunc FuncWithResult[T, R], jobCh <-chan job[T, R], maxRetryWorkerRestart int32) *worker[T, R] {
	return &worker[T, R]{
		parentCtx:             ctx,
		id:                    fmt.Sprintf("worker::%d", index),
		processingFunc:        processingFunc,
		jobCh:                 jobCh,
		maxRetryWorkerRestart: maxRetryWorkerRestart,
	}
}
