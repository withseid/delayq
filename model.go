package delayq

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
