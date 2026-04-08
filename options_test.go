package worker

import (
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
}
