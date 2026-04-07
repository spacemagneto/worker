package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestResult(t *testing.T) {
	t.Parallel()

	t.Run("SuccessResult", func(t *testing.T) {
		resultProcessing := newProcessingResult[struct{}]()

		expectedResult := struct{}{}

		go resultProcessing.processingDone(expectedResult, nil)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		result, err := resultProcessing.WaitResult(ctx)
		assert.NoError(t, err, "Expected no error when processing is successful")
		assert.Equal(t, expectedResult, result, "Expected processing result to match delivered data")

		select {
		case _, ok := <-resultProcessing.ProcessingIsDone():
			assert.False(t, ok, "Expected done channel to be closed, indicating result processing completion")
		default:
			t.Error("Expected ProcessingIsDone channel to be closed after processingDone call")
		}
	})

	t.Run("FailedDelivery", func(t *testing.T) {
		resultProcessing := newProcessingResult[any]()

		expectedError := errors.New("failed processing")

		resultProcessing.processingDone(nil, expectedError)

		result, err := resultProcessing.WaitResult(context.Background())

		assert.ErrorIs(t, err, expectedError, "Expected returned error to be exactly one provided to processingDone")
		assert.Zero(t, result, "Expected result to be a zero value when an error occurs")
	})

	t.Run("ContextTimeout", func(t *testing.T) {
		resultProcessing := newProcessingResult[any]()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		result, err := resultProcessing.WaitResult(ctx)
		assert.ErrorIs(t, err, context.DeadlineExceeded, "Expected a DeadlineExceeded error when context times out")
		assert.Nil(t, result, "Expected a nil result on context timeout")
	})

	t.Run("CheckSyncOnce", func(t *testing.T) {
		resultProcessing := newProcessingResult[any]()

		expectedResult := struct{}{}

		secondExpectedResult := float64(1415)
		expectedError := errors.New("second call got panic")

		resultProcessing.processingDone(expectedResult, nil)
		resultProcessing.processingDone(secondExpectedResult, expectedError)

		result, err := resultProcessing.WaitResult(context.Background())

		assert.Equal(t, expectedResult, result, "Expected first result to be preserved due to sync.Once idempotency")
		assert.NoError(t, err, "Expected second error to be ignored and first nil error to remain")
	})
}
