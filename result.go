package worker

import "sync"

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

func (p *ProcessingResult[R]) processingDone(res R, err error) {
	p.onceDone.Do(func() {
		p.processingRes = res
		p.processingErr = err
		close(p.doneCh)
	})
}

func (p *ProcessingResult[R]) ProcessingIsDone() <-chan struct{} {
	return p.doneCh
}
