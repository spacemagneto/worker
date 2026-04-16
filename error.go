package worker

import (
	"errors"
	"fmt"
	"runtime/debug"
)

var (
	// ErrProcessingFuncIsEmpty occurs when you try to create a pool without providing
	// a valid execution function. We cant process "nothing" so check your constructor.
	ErrProcessingFuncIsEmpty = errors.New("processing func is empty")

	// ErrPoolStopped is returned when you attempt to submit a new job
	// to a pool that has already been shut down. Once the pool is dead, it stays dead.
	ErrPoolStopped = errors.New("worker pool is stopped")
)

// PanicError represents a recovered panic with its original value and call stack.
// This structure is essential for debugging asynchronous failures in workers.
type PanicError struct {
	Err             any
	PanicStackTrace []byte
}

// Error implements the error interface. It provides a human-readable
// description of the panic, ensuring that string-based panics are captured.
func (p *PanicError) Error() string {
	// Previously, the check was based on the assumption that `recover` should only contain an error.
	// Now, after seeing examples of panics where there was a description string instead of errors,
	// I think we should switch to `any`.
	// This will allow us to handle any values during a panic,
	// especially if the panic was triggered not by the method itself but by something inside it,
	// and there is no error but only a description-which is better than nothing.
	// We will also use the `recover stack trace` to understand what might have caused this behavior.
	return fmt.Sprintf("worker panic recover error: %v", p.Err)
}

// Unwrap allows PanicError to play nicely with errors.Is and errors.As.
// If the panic value was an actual error, it returns it for further inspection.
func (p *PanicError) Unwrap() error {
	if err, ok := p.Err.(error); ok {
		return err
	}

	return nil
}

// GetRecoverError converts a recovered value into a formal error interface.
// It captures the current goroutine's stack trace to help trace the root cause
// of the panic inside the worker's processing logic.
func GetRecoverError(rec any) error {
	if rec == nil {
		return nil
	}

	return &PanicError{Err: rec, PanicStackTrace: debug.Stack()}
}
