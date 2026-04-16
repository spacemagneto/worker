package worker

import (
	"context"
	"sync"
)

type ProcessingResult[R any] struct {
	doneCh        chan struct{}
	processingRes R
	processingErr error

	onceDone sync.Once
}

func newProcessingResult[R any]() *ProcessingResult[R] {
	return &ProcessingResult[R]{
		doneCh: make(chan struct{}, 1),
	}
}

// processingDone is called by the worker when it finishes a job - successfully or not.
// The sync.Once makes sure we only ever write once, which matters because cancelAllJob
// and normal completion could theoretically race to call this.
func (p *ProcessingResult[R]) processingDone(res R, err error) {
	p.onceDone.Do(func() {
		p.processingRes = res
		p.processingErr = err
		close(p.doneCh)
	})
}

// ProcessingIsDone returns a channel that gets closed when the job finishes.
// Instead of blocking, you can use this in a select alongside other channels
// handy when you want to do something else while waiting, or race a batch of jobs.
func (p *ProcessingResult[R]) ProcessingIsDone() <-chan struct{} {
	return p.doneCh
}

// WaitResult blocks until the job is done or the context is cancelled.
// It's safe to call from multiple goroutines and always returns the same result
// reading from a closed channel never blocks, so there's no race here.
func (p *ProcessingResult[R]) WaitResult(ctx context.Context) (R, error) {
	select {
	case <-p.doneCh:
		return p.processingRes, p.processingErr

	case <-ctx.Done():
		var res R
		return res, ctx.Err()
	}
}
