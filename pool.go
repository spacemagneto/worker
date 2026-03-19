package worker

import (
	"context"
	"sync"
	"sync/atomic"
)

type Pool struct {
	ctx               context.Context
	contextCancelFunc context.CancelFunc

	taskCh chan any

	maxWorkerRestarts int64

	isStop atomic.Bool
	wg     sync.WaitGroup
}

func New(opts ...Option) *Pool {
	cfg := defaultConfig()

	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	ctx, cancel := context.WithCancel(cfg.context)
	return &Pool{
		ctx:               ctx,
		contextCancelFunc: cancel,
		taskCh:            make(chan any),
		maxWorkerRestarts: int64(cfg.maxWorkerRestarts),
	}
}

func (p *Pool) Run() {}

func (p *Pool) workerLoop() {}

func (p *Pool) Stop() {}
