package delayq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type storage interface {
	redisStorage
}

type implStorage struct {
	redisCli *redis.Client
}

func newStorage(redisConfig RedisConfiguration) (storage, error) {
	redisCli, err := initRedis(redisConfig)
	if err != nil {
		return nil, err
	}

	return &implStorage{
		redisCli: redisCli,
	}, nil
}

type redisStorage interface {
	popFromDelayQueue(topic string, key string) error
	pushToReadyQueue(topic string, job Job) error
	pushToDelayQueue(topic string, job Job) error
	getReadyJob(topic string) (*Job, error)
	putJobPool(topic string, jobID string, value string) error
	migrateExpiredJob(topic string) error
}

func (r *implStorage) migrateExpiredJob(topic string) error {
	readyQueueKey := fmt.Sprintf("%s_%s", RedisReadyQueue, topic)
	delayQueueKey := fmt.Sprintf("%s_%s", RedisDelayQueue, topic)
	currentTime := time.Now().Unix()
	_, err := migrateExpiredJobScript.Run(context.TODO(), r.redisCli,
		[]string{delayQueueKey, readyQueueKey}, []interface{}{currentTime}).StringSlice()
	if err != nil {
		return err
	}
	return nil
}

func (r *implStorage) getReadyJob(topic string) (*Job, error) {

	readyQueueKey := fmt.Sprintf("%s_%s", RedisReadyQueue, topic)
	jobPoolKey := fmt.Sprintf("%s_%s", RedisJobPool, topic)

	res, err := getReadyJobScript.Run(context.TODO(), r.redisCli, []string{readyQueueKey, jobPoolKey}).Result()

	if err != nil {
		return nil, err
	}

	str := res.(string)
	job := Job{}
	err = json.Unmarshal([]byte(str), &job)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (r *implStorage) pushToDelayQueue(topic string, job Job) error {

	delayQueueKey := fmt.Sprintf("%s_%s", RedisDelayQueue, topic)
	jobPoolTopic := fmt.Sprintf("%s_%s", RedisJobPool, topic)
	jobBytes, _ := json.Marshal(job)

	return pushToDelayQueueScript.Run(context.TODO(), r.redisCli, []string{delayQueueKey, jobPoolTopic},
		[]interface{}{job.ID, job.Delay, string(jobBytes)}).Err()
}

func (r *implStorage) pushToReadyQueue(topic string, job Job) error {

	readyQueueKey := fmt.Sprintf("%s_%s", RedisReadyQueue, topic)
	jobPoolTopic := fmt.Sprintf("%s_%s", RedisJobPool, topic)
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return err
	}

	return pushToReadyQueueScript.Run(context.TODO(), r.redisCli, []string{readyQueueKey, jobPoolTopic}, []interface{}{
		job.ID, string(jobBytes),
	}).Err()
}

func (r *implStorage) popFromDelayQueue(topic string, jobID string) error {

	key := fmt.Sprintf("%s_%s", RedisDelayQueue, topic)
	err := r.redisCli.ZRem(context.TODO(), key, jobID).Err()
	if err != nil {
		return err
	}

	return nil
}

func (r *implStorage) putJobPool(topic string, key string, value string) error {

	topic = fmt.Sprintf("%s_%s", RedisJobPool, topic)

	err := r.redisCli.HSet(context.TODO(), topic, key, value).Err()
	if err != nil {
		return err
	}
	return nil
}
