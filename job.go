package worker

type job[T, R any] struct {
	data      T
	jobResult *ProcessingResult[R]
}
