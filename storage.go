package delayq

import (
	"context"
	"encoding/json"
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
	migrateExpiredJob(topic string) error
}

func (r *implStorage) migrateExpiredJob(topic string) error {
	currentTime := time.Now().Unix()
	_, err := migrateExpiredJobScript.Run(context.TODO(), r.redisCli,
		[]string{getDelayQueueKey(topic), getReadyQueueKey(topic)}, []interface{}{currentTime}).StringSlice()
	if err != nil {
		return err
	}
	return nil
}

func (r *implStorage) getReadyJob(topic string) (*Job, error) {

	res, err := getReadyJobScript.Run(context.TODO(), r.redisCli, []string{getReadyQueueKey(topic), getJobPoolKey(topic)}).Result()
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

	jobBytes, _ := json.Marshal(job)

	return pushToDelayQueueScript.Run(context.TODO(), r.redisCli, []string{getDelayQueueKey(topic), getJobPoolKey(topic)},
		[]interface{}{job.ID, job.Delay, string(jobBytes)}).Err()
}

func (r *implStorage) pushToReadyQueue(topic string, job Job) error {

	jobBytes, err := json.Marshal(job)
	if err != nil {
		return err
	}

	return pushToReadyQueueScript.Run(context.TODO(), r.redisCli, []string{getReadyQueueKey(topic), getJobPoolKey(topic)}, []interface{}{
		job.ID, string(jobBytes),
	}).Err()
}

func (r *implStorage) popFromDelayQueue(topic string, jobID string) error {
	return deleteDelayJobScript.Run(context.TODO(), r.redisCli, []string{getDelayQueueKey(topic), getJobPoolKey(topic)},
		[]interface{}{jobID}).Err()
}
