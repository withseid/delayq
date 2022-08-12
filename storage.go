package delayq

import (
	"context"

	"github.com/go-redis/redis/v8"
)

type Storage interface {
	redisStorage
}

type ImplStorage struct {
	redisCli *redis.Client
}

func NewStorage(redisConfig RedisConfiguration) (Storage, error) {
	redisCli, err := InitRedis(redisConfig)
	if err != nil {
		return nil, err
	}

	return &ImplStorage{
		redisCli: redisCli,
	}, nil
}

type redisStorage interface {
	ZAdd(key string, job *Job) error
}

func (r *ImplStorage) ZAdd(key string, job *Job) error {
	return r.redisCli.ZAdd(
		context.TODO(), key,
		&redis.Z{
			Score:  float64(job.Delay),
			Member: job.ID,
		},
	).Err()
}
