package worker

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestPool(t *testing.T) {
	t.Parallel()

	t.Run("NewFuncWithEmptyProcessingFunc", func(t *testing.T) {
		pool, err := NewFunc[any](nil)
		assert.Nil(t, pool, "Expected the pool to be nil when an empty processing function is provided")
		assert.ErrorIs(t, err, ErrProcessingFuncIsEmpty, "Expected ErrProcessingFuncIsEmpty when the NewFunc constructor receives a nil function")
	})

	t.Run("NewFuncWithErrorWithEmptyProcessingFunc", func(t *testing.T) {
		pool, err := NewFuncWithError[any](nil)
		assert.Nil(t, pool, "Expected the pool to be nil when an empty processing function is provided")
		assert.ErrorIs(t, err, ErrProcessingFuncIsEmpty, "Expected ErrProcessingFuncIsEmpty when the NewFuncWithError constructor receives a nil function")
	})

	t.Run("NewResultWithEmptyProcessingFunc", func(t *testing.T) {
		pool, err := NewResult[any, any](nil)
		assert.Nil(t, pool, "Expected the pool to be nil when an empty processing function is provided")
		assert.ErrorIs(t, err, ErrProcessingFuncIsEmpty, "Expected ErrProcessingFuncIsEmpty when the NewResult constructor receives a nil function")
	})

	t.Run("SuccessJobProcessing", func(t *testing.T) {
		processingCounter := 100
		var jobCounter atomic.Int32

		processingFunc := func(context.Context, string) error {
			jobCounter.Add(1)
			return nil
		}

		pool, err := NewFuncWithError(processingFunc, WithWorkers(4), WithQueueSize(10))
		assert.NoError(t, err, "Expected no error when creating a pool with a valid processing function")
		assert.NotNil(t, pool, "Expected pool instance to be successfully initialized")
		assert.False(t, pool.isStop.Load(), "Expected pool to be in an active (not stopped) state upon initialization")

		pool.Run()

		resultList := make([]*ProcessingResult[struct{}], 0, processingCounter)

		for index := range processingCounter {
			res, jobErr := pool.AddJobWithResult("")
			assert.NoError(t, jobErr, fmt.Sprintf("Expected job %d to be accepted by pool", index))

			resultList = append(resultList, res)
		}

		for _, res := range resultList {
			select {
			case _, ok := <-res.ProcessingIsDone():
				assert.False(t, ok, "Expected done channel to be closed, indicating result processing completion")

			case <-time.After(150 * time.Millisecond):
				t.Error("Result processing did not done within the expected time")
			}
		}

		pool.Stop()

		assert.Equal(t, int32(processingCounter), jobCounter.Load(), "Expected final job count to exactly match number of submitted tasks")
		assert.True(t, pool.isStop.Load(), "Expected pool's internal state to be marked as stopped after calling Stop()")
	})

	t.Run("AddJobAfterStopPool", func(t *testing.T) {
		processingFunc := func(_ context.Context, n int) (int, error) {
			return n * 2, nil
		}

		pool, err := NewResult(processingFunc, WithWorkers(4), WithQueueSize(10))
		assert.NoError(t, err, "Expected no error when creating a new pool with valid options")
		assert.NotNil(t, pool, "Expected pool instance to be initialized and not nil")

		assert.False(t, pool.isStop.Load(), "Pool should not be marked as stopped initially")

		pool.Run()

		time.Sleep(30 * time.Millisecond)

		res, err := pool.AddJobWithResult(5)
		assert.NoError(t, err, "Expected job to be accepted by pool while it is running")

		result, err := res.WaitResult(context.Background())
		assert.NoError(t, err, "Expected job to be processed without errors")
		assert.Equal(t, 10, result, "Expected processing result to be correctly calculated (10 * 2)")

		pool.Stop()

		_, poolStopErr := pool.AddJobWithResult(1)
		assert.Error(t, poolStopErr, "Expected an error when attempting to add a job to a stopped pool")
		assert.ErrorIs(t, poolStopErr, ErrPoolStopped, "Expected error to specifically be ErrPoolStopped")
	})

	t.Run("AddJobAndContextDone", func(t *testing.T) {
		processingFunc := func(context.Context, int) { return }

		pool, err := NewFunc(processingFunc, WithWorkers(1), WithQueueSize(1))
		assert.NoError(t, err, "Expected no error when creating a pool with a valid processing function")
		assert.NotNil(t, pool, "Expected pool instance to be successfully initialized")
		assert.False(t, pool.isStop.Load(), "Expected pool to be in an active (not stopped) state upon initialization")

		pool.Run()

		time.Sleep(30 * time.Millisecond)

		pool.ctx.Done()

		contextErr := pool.AddJob(10)
		assert.NoError(t, contextErr, "Expected the job to be accepted successfully since the pool context is still active")

		pool.Stop()
		assert.True(t, pool.isStop.Load(), "Expected pool state to be marked as stopped after calling Stop()")
	})

	t.Run("AddJobWithBlockingJobQueueForContextCanceled", func(t *testing.T) {
		processingFunc := func(ctx context.Context, n int) (int, error) {
			time.Sleep(100 * time.Millisecond)
			return n, nil
		}

		pool, err := NewResult(processingFunc, WithWorkers(1), WithQueueSize(1))
		assert.NoError(t, err, "Expected no error when creating a new pool with valid options")
		assert.NotNil(t, pool, "Expected pool instance to be initialized and not nil")

		pool.Run()

		err = pool.AddJob(1111)
		assert.NoError(t, err, "Expected job to be accepted by pool while it is running")

		err = pool.AddJob(22151)
		assert.NoError(t, err, "Expected job to be accepted by pool while it is running")

		errChan := make(chan error)
		go func() {
			errChan <- pool.AddJob(15151)
		}()

		time.Sleep(20 * time.Millisecond)

		pool.contextCancelFunc()

		submitErr := <-errChan
		assert.ErrorIs(t, submitErr, context.Canceled, "Expected submit to return context.Canceled when pool context is done")

		pool.Stop()
	})
}

func TestPoolSuccessProcessingWithGoLeaks(t *testing.T) {
	defer goleak.VerifyNone(t)

	processingCounter := 20
	var jobCounter atomic.Int32

	processingFunc := func(context.Context, struct{}) error {
		jobCounter.Add(1)
		return nil
	}

	pool, err := NewFuncWithError(processingFunc, WithWorkers(4), WithQueueSize(10))
	assert.NoError(t, err, "Expected no error when creating pool with a cancelable context")
	assert.NotNil(t, pool, "Expected pool instance to be successfully initialized")
	assert.False(t, pool.isStop.Load(), "Pool should not be marked as stopped initially")

	pool.Run()
	pool.Run()

	resultList := make([]*ProcessingResult[struct{}], 0, processingCounter)

	for index := range processingCounter {
		res, jobErr := pool.AddJobWithResult(struct{}{})
		assert.NoError(t, jobErr, fmt.Sprintf("Expected job %d to be accepted by pool", index))

		resultList = append(resultList, res)
	}

	for _, res := range resultList {
		select {
		case _, ok := <-res.ProcessingIsDone():
			assert.False(t, ok, "Expected done channel to be closed, indicating result processing completion")

		case <-time.After(150 * time.Millisecond):
			t.Error("Result processing did not done within the expected time")
		}
	}

	pool.Stop()

	assert.Equal(t, int32(processingCounter), jobCounter.Load(), "Expected number of processed jobs to exactly match number of submitted jobs")
	assert.True(t, pool.isStop.Load(), "Expected pool state to be marked as stopped after calling Stop()")
}

func TestWorkerPoolProcessingWithPanicRecovery(t *testing.T) {
	defer goleak.VerifyNone(t)

	processingFunc := func(_ context.Context, _ int) (int, error) {
		panic("unexpected panic inside job")
	}

	pool, err := NewResult(processingFunc, WithWorkers(1), WithQueueSize(2))
	assert.NoError(t, err, "Expected no error when initializing pool with a panic-prone function")

	pool.Run()

	processingResult, err := pool.AddJobWithResult(1)
	assert.NoError(t, err, "Expected job to be successfully accepted by pool despite potential panic")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, jobErr := processingResult.WaitResult(ctx)
	assert.Error(t, jobErr, "Expected an error to be returned when processing function panics")

	var panicErr *PanicError
	assert.ErrorAs(t, jobErr, &panicErr, "Expected job error to be of type *PanicError")

	pool.Stop()
}

func TestContextCancelStopsPool(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(context.Background())

	pool, err := NewFuncWithError(func(_ context.Context, _ int) error {
		return nil
	}, WithContext(ctx), WithWorkers(2), WithQueueSize(4))
	assert.NoError(t, err, "Expected no error when creating pool with a cancelable context")
	assert.NotNil(t, pool, "Expected pool instance to be successfully initialized")

	pool.Run()

	expectedCount := 10

	for index := range expectedCount {
		err = pool.AddJob(expectedCount)
		assert.NoError(t, err, fmt.Sprintf("Expected job %d to be accepted by pool", index))
	}

	cancel()

	pool.Stop()

	assert.True(t, pool.isStop.Load(), "Expected pool state to be marked as stopped after context cancellation and Stop call")
}
