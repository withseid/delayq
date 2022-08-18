package delayq

import (
	"log"
	"time"
)

type Client struct {
	storage storage
}

func NewClient(config RedisConfiguration) Client {
	storage, err := newStorage(config)
	if err != nil {
		panic(err)
	}

	client := Client{
		storage: storage,
	}
	return client
}

func (c *Client) Dequeue(topic string, jobID string) {
	err := c.storage.popFromDelayQueue(topic, jobID)
	if err != nil {
		log.Fatal(err)
	}
}

func (c *Client) Enqueue(topic string, jobID string, payload []byte, opts ...Option) {
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
			delay := time.Time(opt).Unix()
			job.Delay = delay
		case processInOption:
			delay := time.Now().Add(time.Duration(opt)).Unix()
			job.Delay = delay
		default:

		}
	}

	// TODO: 超时处理
	if job.Delay == 0 {
		err := c.storage.pushToReadyQueue(topic, *job)
		if err != nil {
			log.Fatal("[delayq pushToReadyQueue error]: ", err)
		}
		return
	}

	err := c.storage.pushToDelayQueue(topic, *job)
	if err != nil {
		log.Fatal("[delayq pushToDelayQueue error]: ", err)
	}
}
