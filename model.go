package delayq

type RedisConfiguration struct {
	Host string
	Port string
}

var DelayBucket = "delay_bucket"
