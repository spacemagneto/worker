package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWorker(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	t.Run("SuccessCreateWorker", func(t *testing.T) {
		jobCh := make(chan job[int, int])

		ctx := context.Background()
		processingFunc := func(_ context.Context, n int) (int, error) { return n, nil }
		index := 7

		expectedName := fmt.Sprintf("worker::%d", index)

		wr := newWorker(ctx, index, processingFunc, jobCh, 5, logger)

		assert.Equal(t, expectedName, wr.id)
		assert.Equal(t, int32(5), wr.maxRetryWorkerRestart)
		assert.Equal(t, int32(0), wr.restartCounter.Load())
	})

	t.Run("SuccessJobProcessing", func(t *testing.T) {
		jobCh := make(chan job[int, int], 1)

		ctx, cancel := context.WithCancel(context.Background())

		processingFunc := func(_ context.Context, n int) (int, error) { return n * 2, nil }

		wr := newWorker(ctx, 1, processingFunc, jobCh, 3, logger)
		res := newProcessingResult[int]()

		jobCh <- job[int, int]{data: 10, jobResult: res}

		var wg sync.WaitGroup

		wg.Add(1)
		go wr.start(&wg)

		result, err := res.WaitResult(ctx)
		assert.NoError(t, err, "Expected job to be processed without errors")
		assert.Equal(t, 20, result, "Expected processing result to be correctly calculated (10 * 2)")

		cancel()

		wg.Wait()
	})

	t.Run("SuccessWorkerRestartAfterPanic", func(t *testing.T) {
		jobCh := make(chan job[int, int], 1)

		processingFunc := func(_ context.Context, n int) (int, error) { return n * 2, nil }

		wr := newWorker(nil, 1, processingFunc, jobCh, 3, logger)

		var wg sync.WaitGroup

		wg.Add(1)
		go wr.start(&wg)

		<-time.After(10 * time.Millisecond)

		assert.Equal(t, int32(3), wr.restartCounter.Load(), "Expected the restart counter to be exactly 1")

		wg.Wait()
	})
}
