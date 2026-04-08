package worker

import (
	"context"
	"errors"
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

func TestCancelAllJob(t *testing.T) {
	expectedSize := 10
	jobCh := make(chan job[int, int], expectedSize)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	processingFunc := func(_ context.Context, n int) (int, error) { return n * 2, nil }

	wr := newWorker(ctx, 1, processingFunc, jobCh, 3, slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	processingList := make([]*ProcessingResult[int], 0, expectedSize)

	for range expectedSize {
		result := newProcessingResult[int]()
		processingList = append(processingList, result)

		jobCh <- job[int, int]{data: 1, jobResult: result}
	}

	wr.cancelAllJob()

	for index, result := range processingList {
		res, err := result.WaitResult(ctx)

		assert.Error(t, err, fmt.Sprintf("Expected an error for job %d because pool was stopped", index))
		assert.ErrorIs(t, err, context.Canceled, fmt.Sprintf("Expected error for job %d to be context.Canceled", index))
		assert.NotNil(t, res, fmt.Sprintf("Expected result for job %d to be zero value of type, not nil", index))

		select {
		case _, ok := <-result.ProcessingIsDone():
			assert.False(t, ok, fmt.Sprintf("Expected done channel for job %d to be closed, indicating result processing completion", index))
		default:
			t.Errorf("Expected ProcessingIsDone channel for job %d to be closed after cancelAllJob call", index)
		}
	}
}

func TestJobWithErrorFunc(t *testing.T) {
	jobCh := make(chan job[int, int], 1)

	ctx, cancel := context.WithCancel(context.Background())

	sentinelErr := errors.New("failed processing")
	processingFunc := func(_ context.Context, n int) (int, error) {
		return 0, sentinelErr
	}

	wr := newWorker(ctx, 1, processingFunc, jobCh, 3, slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	res := newProcessingResult[int]()

	var wg sync.WaitGroup

	wg.Add(1)
	go wr.start(&wg)

	errSignal := make(chan error, 1)
	expectedErrorFunc := func(err error) { errSignal <- err }

	jobCh <- job[int, int]{data: 10, jobResult: res, errorFunc: expectedErrorFunc}

	result, err := res.WaitResult(ctx)

	var capturedErr error
	select {
	case capturedErr = <-errSignal:
		assert.Error(t, err, "Expected processing error to be returned from WaitResult")
		assert.ErrorIs(t, capturedErr, sentinelErr, "Expected the error passed to errorFunc to match the sentinel error")
		assert.Equal(t, 0, result, "Expected result to be zero value on error")

	case <-time.After(1 * time.Second):
		t.Fatal("Expected errorFunc to be called, but timed out")
	}

	cancel()

	wg.Wait()
}
