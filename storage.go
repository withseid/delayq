package delayq

import (
	"context"
	"encoding/json"
	"fmt"

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
}

func (r *implStorage) getReadyJob(topic string) (*Job, error) {

	// 从 ready_queue 中获取一个 jobID
	// 以下三个 redis 操作，改为 lua script 执行
	jobID, err := r.redisCli.RPop(context.TODO(), topic).Result()
	if err != nil {
		return nil, err
	}

	jobStr, err := r.redisCli.HGet(context.TODO(), topic, jobID).Result()
	if err != nil {
		return nil, err
	}
	err = r.redisCli.HDel(context.TODO(), topic, jobID).Err()
	if err != nil {
		return nil, err
	}
	job := Job{}
	err = json.Unmarshal([]byte(jobStr), &job)
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

	return pushToReadyQueue.Run(context.TODO(), r.redisCli, []string{readyQueueKey, jobPoolTopic}, []interface{}{
		job.ID, string(jobBytes),
	}).Err()
}

func (r *implStorage) popFromDelayQueue(topic string, value string) error {

	key := fmt.Sprintf("%s_%s", RedisDelayQueue, topic)
	err := r.redisCli.HDel(context.TODO(), key, value).Err()
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
