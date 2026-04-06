package worker

import (
	"context"
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
}
