package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
)

type worker[T, R any] struct {
	parentCtx             context.Context
	id                    string
	processingFunc        Processor[T, R]
	jobCh                 <-chan job[T, R]
	maxRetryWorkerRestart int32
	restartCounter        atomic.Int32

	logger *slog.Logger
}

func newWorker[T, R any](ctx context.Context, index int, processingFunc Processor[T, R], jobCh <-chan job[T, R], maxRetryWorkerRestart int32, logger *slog.Logger) *worker[T, R] {
	return &worker[T, R]{
		parentCtx:             ctx,
		id:                    fmt.Sprintf("worker::%d", index),
		processingFunc:        processingFunc,
		jobCh:                 jobCh,
		maxRetryWorkerRestart: maxRetryWorkerRestart,
		logger:                logger,
	}
}

func (w *worker[T, R]) start(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		tryRestart, err := w.workerLoop()
		if !tryRestart {
			w.logger.Info("worker stopped, no restart needed")
			return
		}

		if err != nil {
			w.logger.Error("restarting worker after panic", "error", err)
		}

		attempt := w.restartCounter.Add(1)

		if attempt >= w.maxRetryWorkerRestart {
			w.logger.Error("maximum restart attempts reached", "worker", slog.String("worker::ID", w.id))
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
			w.logger.Info("parent context is done")
			// To prevent open channels that are waiting for a task to complete from leaking,
			// if context is closed (worker pool is stopped),
			// a method will be called that forces an empty result but closes channel
			w.cancelAllJob()

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

func (w *worker[T, R]) cancelAllJob() {
	for {
		select {
		case j := <-w.jobCh:
			if j.jobResult != nil {
				var zero R
				j.jobResult.processingDone(zero, w.parentCtx.Err())
			}

		default:
			return
		}
	}
}
