package delayq

type JobState int

const (
	JobReady JobState = iota
	JobDelay
	JobReserved
	JobDeleted
)

type Job struct {
	Topic string
	ID    string
	Delay int64
	TTR   int64
	Boday []byte
	State JobState
}

type JobPool struct {
	pool []Job
}

// Delay Bucket => zset

// Ready Queue => redis list
