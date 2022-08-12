package delayq

import (
	"time"

	s "github.com/deckarep/golang-set"
)

type Client struct {
	storage Storage
	jobPool map[string]*Job

	readyQueue map[string]s.Set
}

func NewClient(config RedisConfiguration) Client {
	storage, err := NewStorage(config)
	if err != nil {
		panic(err)
	}

	client := Client{
		storage:    storage,
		jobPool:    make(map[string]*Job),
		readyQueue: make(map[string]s.Set),
	}
	return client
}

func (c *Client) Dequeue(topic string, jobID string) {
	for k := range c.jobPool {
		if k == jobID {
			delete(c.jobPool, jobID)
		}
	}

}

func (c *Client) Enqueue(payload []byte, topic string, jobID string, opts ...Option) error {
	job := &Job{
		Topic: topic,
		ID:    jobID,
		Boday: payload,
	}

	for _, opt := range opts {
		switch opt := opt.(type) {
		case timeoutOption:
			job.TTR = time.Duration(opt).Milliseconds()
		case processAtOption:
			delay := time.Time(opt).UnixNano()
			job.Delay = delay
		case processInOption:
			delay := time.Now().Add(time.Duration(opt)).UnixNano()
			job.Delay = delay
		default:

		}
	}
	if job.Delay == 0 {
		job.State = JobReady
	} else {
		job.State = JobDelay
	}

	// 如果不是延迟任务，马上加入就绪队列
	if job.State == JobReady {
		queue, ok := c.readyQueue[topic]
		if !ok {
			queue = s.NewSet()
		}
		queue.Add(jobID)

		c.readyQueue[topic] = queue
	} else {
		c.storage.ZAdd(DelayBucket, job)
	}

	return nil
}
