# worker

A generic, lock-free, type-safe worker pool for Go with panic recovery, job timeouts, optional result tracking, and configurable restart logic.

## Features

- Four Purpose-Built Modes: Choose between simple tasks, data handlers, error-aware workers, or full request-response processors.
- Per-job timeouts and error callbacks
- Worker panic recovery with configurable restart attempts
- Graceful shutdown - drains in-flight jobs and closes pending result channels
- Structured logging via `log/slog`

```bash
go get github.com/spacemagneto/worker
```

## Constructors

| Constructor   | Best for                                           | Input (T) | Result(R) |
|---------------|----------------------------------------------------|-----------|-----------|
| `NewTaskPool` | Fire-and-forget triggers without any input data.   | None      | None      |
| `NewPool`     | Mass data processing where results aren't needed   | Required  | None      |
| `NewErrPool`  | Reliable delivery where you need to track failures | Required  | None      |
| `New`         | Full Pipelines requiring a returned value (Result) | Required  | Required  |

---

## Usage

### 1. `NewTaskPool` - fire and forget.

Use this option if you want the task to run, but you don't need error reports, results, or any input data.

```go
package main
 
import (
	"context"
	"log"
	"sync/atomic"
 
	"github.com/spacemagneto/worker"
)
 
func main() {
	processingCounter := 10
	var jobCounter atomic.Int32

	processingFunc := func(context.Context) {
		jobCounter.Add(1)
	}

	pool, err := worker.NewTaskPool(processingFunc, worker.WithWorkers(4), worker.WithQueueSize(16))
	if err != nil {
		log.Fatal(err)
	}
 
	pool.Run()
	defer pool.Stop()

	for range processingCounter {
		// You can pass anything you want—it really doesn’t matter. This type of handler allows you to pass anything, 
		// but it doesn’t make sense because the method only requires a context and no input arguments.
		err = pool.AddJob("") 
	}
}
```

### 2. `NewPool` — fire and forget

Use this when you want work done and don't need errors or results back.

```go
package main
 
import (
	"context"
	"fmt"
	"log"
 
	"github.com/spacemagneto/worker"
)
 
func main() {
	processingFunc := func(ctx context.Context, input string) {
		fmt.Println("processing:", input)
	}

	pool, err := worker.NewPool(processingFunc, worker.WithWorkers(4), worker.WithQueueSize(16))
	if err != nil {
		log.Fatal(err)
	}
 
	pool.Run()
	defer pool.Stop()
 
	words := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for _, w := range words {
		if err = pool.AddJob(w); err != nil {
			log.Println("AddJob failed:", err)
		}
	}
}
```
 
---

### 3. `NewErrorPool` — handle errors per job

Use this when the processing function can fail and you want to react to errors.
Errors can be observed globally via the pool-level logger or per-job via `WithErrorFunc`.

```go
package main
 
import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
 
	"github.com/spacemagneto/worker"
)
 
func processOrder(ctx context.Context, orderID int) error {
	if orderID <= 0 {
		return fmt.Errorf("invalid order id: %d", orderID)
	}

	fmt.Printf("order %d processed\n", orderID)
	return nil
}
 
func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
 
	pool, err := worker.NewErrorPool(
		processOrder,
		worker.WithWorkers(4),
		worker.WithQueueSize(32),
		worker.WithLogger(logger),
	)
	if err != nil {
		log.Fatal(err)
	}
 
	pool.Run()
	defer pool.Stop()
 
	orders := []int{1, 2, -1, 3, 0, 4}
	for _, id := range orders {
		submitErr := pool.AddJob(id, worker.WithErrorFunc(func(err error) {
			log.Printf("order %d failed: %v\n", id, err)
		}))

		if submitErr != nil {
			if errors.Is(submitErr, worker.ErrPoolStopped) {
				log.Println("pool stopped, aborting submission")
				break
			}

			log.Println("unexpected submit error:", submitErr)
		}
	}
}
```
 
---

### 4. `New` — await a typed result per job

Use this when you need the output of each job.
`AddJobWithResult` returns a `*ProcessingResult[R]` you can wait on.

```go
package main
 
import (
	"context"
	"fmt"
	"log"
	"time"
 
	"github.com/spacemagneto/worker"
)
 
func square(ctx context.Context, n int) (int, error) {
	if n < 0 {
		return 0, fmt.Errorf("negative input: %d", n)
	}

	return n * n, nil
}
 
func main() {
	pool, err := worker.New(square, worker.WithWorkers(4), worker.WithQueueSize(16))
	if err != nil {
		log.Fatal(err)
	}
 
	pool.Run()
	defer pool.Stop()
 
	res, err := pool.AddJobWithResult(9)
	if err != nil {
		log.Fatal("AddJobWithResult:", err)
	}
 
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
 
	value, err := res.WaitResult(ctx)
	if err != nil {
		log.Println("job error:", err)
	} else {
		fmt.Println("9^2 =", value)
	}
 
	inputs := []int{2, 3, 4, 5, 6}
	results := make([]*worker.ProcessingResult[int], len(inputs))
 
	for i, n := range inputs {
		r, submitErr := pool.AddJobWithResult(n)
		if submitErr != nil {
			log.Fatalf("submit %d: %v", n, submitErr)
		}

		results[i] = r
	}
 
	bgCtx := context.Background()
	for i, r := range results {
		v, jobErr := r.WaitResult(bgCtx)
		if jobErr != nil {
			fmt.Printf("%d² = error: %v\n", inputs[i], jobErr)
		} else {
			fmt.Printf("%d² = %d\n", inputs[i], v)
		}
	}
 
	processingFunc := func(ctx context.Context, payload []byte) ([]byte, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case <-time.After(10 * time.Millisecond):
			return payload, nil
		}
	}
	
	slowPool, err := worker.New(processingFunc, worker.WithWorkers(2))
	if err != nil {
		log.Fatal(err)
	}
 
	slowPool.Run()
	defer slowPool.Stop()
 
	timedRes, err := slowPool.AddJobWithResult([]byte("hello"), worker.WithTimeout(5*time.Second))
	if err != nil {
		log.Fatal(err)
	}
 
	out, err := timedRes.WaitResult(context.Background())
	if err != nil {
		log.Println("timed job error:", err)
	} else {
		fmt.Println("timed job result:", string(out))
	}
}
```
 
---

### 5. `ProcessingIsDone` — react to completion via `select`

`WaitResult` blocks the calling goroutine until the job finishes.
`ProcessingIsDone` gives you the raw channel so you can use it in a `select`
alongside other channels — for example to race a job against a deadline,
handle multiple jobs concurrently, or integrate with an existing event loop.

```go
package main
 
import (
	"context"
	"fmt"
	"log"
	"time"
 
	"github.com/spacemagneto/worker"
)
 
func main() {
	processingFunc := func(_ context.Context, n int) (int, error) {
		time.Sleep(time.Duration(n) * time.Millisecond)
		return n * 2, nil
	}
	
	pool, err := worker.New(processingFunc, worker.WithWorkers(4), worker.WithQueueSize(16))
	if err != nil {
		log.Fatal(err)
	}
 
	pool.Run()
	defer pool.Stop()
 
	res, err := pool.AddJobWithResult(50)
	if err != nil {
		log.Fatal(err)
	}
 
	deadline := time.After(200 * time.Millisecond)
 
	select {
	case <-res.ProcessingIsDone():
		value, jobErr := res.WaitResult(context.Background())
		if jobErr != nil {
			log.Println("job error:", jobErr)
		} else {
			fmt.Println("result: ", value) // 100
		}
 
	case <-deadline:
		fmt.Println("gave up waiting")
	}
 
	jobs := []int{80, 20, 50}
	results := make([]*worker.ProcessingResult[int], len(jobs))
	for i, n := range jobs {
		r, submitErr := pool.AddJobWithResult(n)
		if submitErr != nil {
			log.Fatal(submitErr)
		}

		results[i] = r
	}
 
	// Collect results in completion order, not submission order.
	// Each job signals via ProcessingIsDone; we read the value immediately after.
	remaining := len(results)
	finished := make([]bool, len(results))
	timeout := time.After(5 * time.Second)
 
	for remaining > 0 {
		for i, r := range results {
			if finished[i] {
				continue
			}

			select {
			case <-r.ProcessingIsDone():
				value, jobErr := r.WaitResult(context.Background())
				if jobErr != nil {
					fmt.Printf("job input=%d error: %v\n", jobs[i], jobErr)
				} else {
					fmt.Printf("job input=%d done → %d\n", jobs[i], value)
				}

				finished[i] = true
				remaining--
			case <-timeout:
				fmt.Println("timed out waiting for jobs")
				return
			default:
			}
		}
	}
}
```

The key difference from `WaitResult`:

|               | `WaitResult(ctx)`         | `ProcessingIsDone()`                            |
|---------------|---------------------------|-------------------------------------------------|
| Blocks caller | Yes                       | No — returns channel immediately                |
| Cancellable   | Via `ctx`                 | Via `select` with any channel                   |
| Reads result  | Yes, returns `(R, error)` | No — call `WaitResult` after the channel closes |
| Use case      | Simple sequential wait    | Select across multiple jobs or events           |
 
---

## Options

### Pool options

| Option               | Default                | Description                                              |
|----------------------|------------------------|----------------------------------------------------------|
| `WithContext(ctx)`   | `context.Background()` | Parent context — pool stops automatically when cancelled |
| `WithWorkers(n)`     | `runtime.NumCPU()`     | Number of worker goroutines                              |
| `WithQueueSize(n)`   | `workers * 2`          | Job queue buffer capacity                                |
| `WithMaxRestarts(n)` | `3`                    | Max worker restarts after a loop-level panic             |
| `WithLogger(logger)` | JSON logger to stderr  | Custom `*slog.Logger`                                    |

### Per-job options

| Option              | Description                                                                      |
|---------------------|----------------------------------------------------------------------------------|
| `WithTimeout(d)`    | Cancels the job's context after `d`; handler receives `context.DeadlineExceeded` |
| `WithErrorFunc(fn)` | Called from the worker goroutine when the handler returns an error               |

Both options can be combined:

```go
_ = pool.AddJob(payload,
	worker.WithTimeout(2*time.Second),
	worker.WithErrorFunc(func(err error) {
		log.Println("job error:", err)
	}),
)
```
 
---

## Errors

| Error                             | When                                                |
|-----------------------------------|-----------------------------------------------------|
| `worker.ErrProcessingFuncIsEmpty` | `nil` was passed as the processing function         |
| `worker.ErrPoolStopped`           | `AddJob` / `AddJobWithResult` called after `Stop()` |

Panics inside handler functions are caught and wrapped in `*worker.PanicError`,
which includes a stack trace and implements `Unwrap()` for use with `errors.Is` / `errors.As`:

```go
var pe *worker.PanicError
if errors.As(err, &pe) {
	fmt.Println("panic value:", pe.Err)
	fmt.Println("stack trace:\n", string(pe.PanicStackTrace))
}
```

---

`Stop()` is safe to call multiple times.
If the parent context passed via `WithContext` is cancelled, `Stop()` is triggered automatically.


## License

This package is licensed under the Apache License, Version 2.0. See the LICENSE file for details.

