package worker

import (
	"context"
	"fmt"
	"sync"
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

func (w *worker[T, R]) start(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		tryRestart, err := w.workerLoop()
		if !tryRestart {
			return
		}

		if err != nil {
			// TODO: and?
		}

		attempt := w.restartCounter.Add(1)

		if attempt >= w.maxRetryWorkerRestart {
			return
		}
	}
}

func (w *worker[T, R]) workerLoop() (isRestart bool, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			isRestart = true
			err = GetRecoverError(rec)
		}
	}()

	for {
		select {
		case <-w.parentCtx.Done():
			// TODO: need close all result processing channels
			return

		case newJob, ok := <-w.jobCh:
			if ok {
				w.jobProcessing(newJob)
			}

			// TODO: Overall, we might be able to get out of this, but it's not a sure thing.
		}
	}
}

func (w *worker[T, R]) jobProcessing(j job[T, R]) {
	processingCtx := w.parentCtx
	var processingCtxCancel context.CancelFunc

	if j.timeout > 0 {
		processingCtx, processingCtxCancel = context.WithTimeout(w.parentCtx, j.timeout)
		defer processingCtxCancel()
	}

	res, err := func() (result R, err error) {
		defer func() {
			if recoverErr := recover(); recoverErr != nil {
				err = GetRecoverError(recoverErr)
			}
		}()

		return w.processingFunc(processingCtx, j.data)
	}()

	if j.jobResult != nil {
		j.jobResult.processingDone(res, err)
	}

	if err != nil {
		if j.errorFunc != nil {
			j.errorFunc(err)
		}
	}
}
