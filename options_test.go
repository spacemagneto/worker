package worker

import (
	"context"
	"log/slog"
	"os"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPoolOptions(t *testing.T) {
	t.Parallel()

	t.Run("DefaultConfig", func(t *testing.T) {
		cfg := defaultConfig()

		assert.NotNil(t, cfg.context, "Expected default context to be initialized and not nil")
		assert.Equal(t, runtime.NumCPU(), cfg.workers, "Expected default worker count to match number of CPUs")
		assert.Equal(t, runtime.NumCPU()*2, cfg.queueSize, "Expected default queue size to be double worker count")
		assert.Equal(t, 3, cfg.maxWorkerRestarts, "Expected default maximum worker restarts to be exactly 3")
		assert.NotNil(t, cfg.logger, "Expected default logger to be initialized and not nil")
	})

	t.Run("CustomPoolConfig", func(t *testing.T) {
		cfg := defaultConfig()

		expectedRestarts := 10
		expectedCtx := context.TODO()
		expectedWorkers := 10
		expectedQueueSize := 1024

		opts := &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}

		expectedLogger := slog.New(slog.NewJSONHandler(os.Stdout, opts))

		WithMaxRestarts(expectedRestarts)(&cfg)
		WithContext(expectedCtx)(&cfg)
		WithWorkers(expectedWorkers)(&cfg)
		WithQueueSize(expectedQueueSize)(&cfg)
		WithLogger(expectedLogger)(&cfg)

		assert.Equal(t, expectedRestarts, cfg.maxWorkerRestarts, "Expected maximum worker restarts to be upsert to specified custom value")
		assert.Equal(t, expectedCtx, cfg.context, "Expected configuration context to be upsert with provided custom context")
		assert.Equal(t, expectedWorkers, cfg.workers, "Expected worker count to be updated to specified custom value")
		assert.Equal(t, expectedQueueSize, cfg.queueSize, "Expected queue size to be updated to specified custom value")
		assert.Equal(t, expectedLogger, cfg.logger, "Expected configuration logger to be updated to specified custom slog instance")
	})
}
