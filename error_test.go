package worker

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestError(t *testing.T) {
	t.Parallel()

	t.Run("ErrorInterfaceWithDatabaseError", func(t *testing.T) {
		err := errors.New("database error")
		panicErr := &PanicError{
			Err:             err,
			PanicStackTrace: []byte("debug stack trace"),
		}

		expected := fmt.Sprintf("worker panic recover error: %v", err)
		assert.Equal(t, expected, panicErr.Error(), "Error() should correctly format message when Err is an error type")
	})

	t.Run("SimulatedStringPanicError", func(t *testing.T) {
		err := "unexpected nil pointer"
		panicErr := &PanicError{
			Err:             err,
			PanicStackTrace: []byte("debug stack trace"),
		}

		expected := fmt.Sprintf("worker panic recover error: %s", err)
		assert.Equal(t, expected, panicErr.Error(), "Error() should handle plain string descriptions from recover()")
	})

	t.Run("UnwrapError", func(t *testing.T) {
		err := errors.New("database error")
		panicErr := &PanicError{Err: err}

		unwrapped := panicErr.Unwrap()
		assert.NotNil(t, unwrapped, "Unwrap() should not return nil when Err contains an error")
		assert.Equal(t, err, unwrapped, "Unwrap() should return original database error for use with errors.Is/As")
	})

	t.Run("IncorrectErrorForUnwrap", func(t *testing.T) {
		panicErr := &PanicError{Err: struct{}{}}

		unwrapped := panicErr.Unwrap()
		assert.Nil(t, unwrapped, "Unwrap() must return nil if panic value is not a error")
	})

	t.Run("CheckErrorIs", func(t *testing.T) {
		err := errors.New("database error")
		panicErr := &PanicError{Err: err}

		assert.ErrorIs(t, panicErr, err, "Standard errors.Is should be able to find wrapped error inside PanicError")
	})
}
