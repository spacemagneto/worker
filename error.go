package worker

import (
	"fmt"
	"runtime/debug"
)

type PanicError struct {
	Err             any
	PanicStackTrace []byte
}

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

func (p *PanicError) Unwrap() error {
	if err, ok := p.Err.(error); ok {
		return err
	}

	return nil
}

func GetRecoverError(rec any) error {
	if rec == nil {
		return nil
	}

	return &PanicError{Err: rec, PanicStackTrace: debug.Stack()}
}
