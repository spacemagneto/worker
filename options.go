package worker

type config struct {
	workerCount       int
	queueSize         int
	maxWorkerRestarts int
}
