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

func TestGetRecoverError(t *testing.T) {
	cases := []struct {
		name        string
		input       any
		expectedNil bool
		checkErrMsg string
	}{
		{
			name:        "Return nil when recover is nil",
			input:       nil,
			expectedNil: true,
		},
		{
			name:        "Handle string panic",
			input:       "something went wrong",
			expectedNil: false,
			checkErrMsg: "worker panic recover error: something went wrong",
		},
		{
			name:        "Handle error object panic",
			input:       errors.New("internal connection error"),
			expectedNil: false,
			checkErrMsg: "worker panic recover error: internal connection error",
		},
		{
			name:        "Handle non-standard types (int)",
			input:       500,
			expectedNil: false,
			checkErrMsg: "worker panic recover error: 500",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			err := GetRecoverError(tt.input)

			if tt.expectedNil {
				assert.NoError(t, err, "Expected nil error when input is nil (no panic occurred)")
				return
			}

			assert.NotNil(t, err, "Expected a non-nil error for a non-nil recover value")
			assert.Equal(t, tt.checkErrMsg, err.Error(), "Error message should match expected formatted output")

			var pErr *PanicError
			ok := errors.As(err, &pErr)
			assert.True(t, ok, "Returned error should be of type *PanicError for further inspection")
			assert.NotNil(t, pErr.PanicStackTrace, "Stack trace should be captured and not be nil")
			assert.Contains(t, string(pErr.PanicStackTrace), "goroutine", "Stack trace should contain standard debug.Stack() keywords")
		})
	}
}
