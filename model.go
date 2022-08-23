package delayq

import "fmt"

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

func getDelayQueueKey(topic string) string {
	return fmt.Sprintf("%s_%s", RedisDelayQueue, topic)
}

func getReadyQueueKey(topic string) string {
	return fmt.Sprintf("%s_%s", RedisReadyQueue, topic)
}

func getJobPoolKey(topic string) string {
	return fmt.Sprintf("%s_%s", RedisJobPool, topic)
}
