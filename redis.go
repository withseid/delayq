package delayq

import (
	"fmt"

	"github.com/go-redis/redis"
)

var RedisCli *redis.Client

func InitRedis() {
	config := RedisConfiguration{
		Host: "192.168.89.160",
		Port: "6379",
	}
	addr := fmt.Sprintf("%s:%s", config.Host, config.Port)
	redisClient := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	})
	_, err := redisClient.Ping().Result()
	if err != nil {
		panic(err)
	}
	RedisCli = redisClient
}
