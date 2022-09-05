package delayq

import "fmt"

type RedisConfiguration struct {
	Host string
	Port string
}

type Job struct {
	Topic      string
	ID         string
	Delay      int64
	MaxRetry   int64
	RetryCount int64
	TTR        int64
	Boday      []byte
}

var (
	RedisDelayQueue          = "delayq:dq"
	RedisJobPool             = "delayq:jp"
	RedisReadyQueue          = "delayq:rq"
	RedisProcessQueue        = "delayq:process"
	serverClosed      uint32 = 1
)

func getProcessQueueKey(topic string) string {
	return fmt.Sprintf("%s:%s", RedisProcessQueue, topic)
}
func getDelayQueueKey(topic string) string {
	return fmt.Sprintf("%s:%s", RedisDelayQueue, topic)
}

func getReadyQueueKey(topic string) string {
	return fmt.Sprintf("%s:%s", RedisReadyQueue, topic)
}

func getJobPoolKey(topic string) string {
	return fmt.Sprintf("%s:%s", RedisJobPool, topic)
}
