package delayq

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

func InitRedis(config RedisConfiguration) (*redis.Client, error) {

	addr := fmt.Sprintf("%s:%s", config.Host, config.Port)
	redisCli := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	})

	_, err := redisCli.Ping(context.Background()).Result()
	if err != nil {
		panic(err)
	}

	return redisCli, nil
}
