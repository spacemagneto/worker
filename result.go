package worker

import "sync"

type ProcessingResult[R any] struct {
	doneCh        chan struct{}
	processingRes R
	processingErr error

	onceDone sync.Once
}
