package delayq

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"golang.org/x/sync/semaphore"
)

func TestDelayQServer(t *testing.T) {
	config := RedisConfiguration{
		Host: "192.168.89.160",
		Port: "6379",
	}

	space := Space{}
	worker := Worker{
		TopicName:   space.Topic(),
		Handler:     &space,
		Concurrency: 1,
		WorkerPool:  *semaphore.NewWeighted(10),
	}
	workers := []*Worker{&worker}
	s := NewServer(config, workers)
	s.Run(context.Background())
}

func (s *Space) Topic() string {
	return spaceExpiredTopic
}

func (s *Space) Execute(ctx context.Context, payload []byte) error {

	space := Space{}
	err := json.Unmarshal(payload, &space)
	if err != nil {
		return err
	}

	fmt.Println("[Execute] Space: ", space)
	return nil
}
