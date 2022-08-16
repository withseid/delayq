package delayq

import (
	"context"
	"encoding/json"
	"fmt"
)

type RedisConfiguration struct {
	Host string
	Port string
}

type Job struct {
	Topic string
	ID    string
	Delay int64
	TTR   int64
	Boday []byte
}

var (
	RedisDelayQueue        = "delayQ_delay_queue"
	RedisJobPool           = "delayQ_job_pool"
	RedisReadyQueue        = "delayQ_ready_queue"
	serverClosed    uint32 = 1
)
var spaceExpiredTopic = "space_expired"

type Space struct {
	ID     string
	UserID string
	Phone  string
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
