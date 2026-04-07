package worker

import "time"

type job[T, R any] struct {
	data      T
	jobResult *ProcessingResult[R]

	timeout   time.Duration
	errorFunc func(error)
}
