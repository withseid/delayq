package delayq

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	uuid "github.com/satori/go.uuid"
	"golang.org/x/sync/semaphore"
)

func TestMigrate(t *testing.T) {
	config := RedisConfiguration{
		Host: "192.168.89.160",
		Port: "6379",
	}
	cli, err := initRedis(config)
	if err != nil {
		t.Fatal(err)
	}
	readyQueueKey := fmt.Sprintf("%s_space_expired", RedisReadyQueue)
	ti := time.Now().Unix()

	jobIDs, err := migrateExpiredJobScript.Run(context.TODO(), cli,
		[]string{"delayQ_delay_queue_space_expired", readyQueueKey},
		[]interface{}{ti}).StringSlice()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("jobIDs size: ", len(jobIDs))
	fmt.Println("jobIDs: ", jobIDs)
}

func TestTime(t *testing.T) {
	now := time.Now()
	fmt.Println(now.Unix())
	// on := now.Add(time.Duration(time.Minute)).Unix()
	// fmt.Println(on)
}

func TestTicker(t *testing.T) {
	ticker := time.NewTicker(time.Duration(time.Second * 1))
	for {
		select {
		case <-ticker.C:
			fmt.Println(time.Now())
		}
	}
}

func TestGetReadyJob(t *testing.T) {
	config := RedisConfiguration{
		Host: "192.168.89.160",
		Port: "6379",
	}
	cli, err := initRedis(config)
	if err != nil {
		t.Fatal(err)
	}
	job := Job{}
	res, err := getReadyJobScript.Run(context.TODO(), cli,
		[]string{"delayQ_ready_queue_space_expired", "delayQ_job_pool_space_expired"}).Result()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("res: ", res)
	str := res.(string)
	err = json.Unmarshal([]byte(str), &job)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("job: ", string(job.Boday))

}

func TestSemaphoreWeight(t *testing.T) {
	ctx := context.Background()
	var sema = semaphore.NewWeighted(int64(4))
	for i := 0; i < 10; i++ {
		err := sema.Acquire(ctx, 1)
		if err != nil {
			panic(err)
		}

		go func(i int) {
			defer sema.Release(1)
			time.Sleep(time.Second * 5)
			t.Log(i)
		}(i)
	}

	err := sema.Acquire(ctx, 4)
	if err != nil {
		panic(err)
	}
}

func TestRedisHash(t *testing.T) {
	config := RedisConfiguration{
		Host: "127.0.0.1",
		Port: "6379",
	}
	cli, err := initRedis(config)
	if err != nil {
		panic(err)
	}

	topicID := "topic1"
	jobID := uuid.NewV4().String()
	j := Job{
		Topic: topicID,
		ID:    jobID,
		Delay: 100,
	}
	b, _ := json.Marshal(j)
	err = cli.HSet(context.TODO(), topicID, jobID, string(b)).Err()
	if err != nil {
		panic(err)
	}

	topicID = "topic2"
	jobID = uuid.NewV4().String()
	j = Job{
		Topic: topicID,
		ID:    jobID,
		Delay: 300,
	}

	b, _ = json.Marshal(j)

	err = cli.HSet(context.TODO(), topicID, jobID, string(b)).Err()
	if err != nil {
		panic(err)
	}

}

// func TestInitRedis(t *testing.T) {

// 	InitRedis()

// 	for i := 0; i < 10; i++ {
// 		s := time.Duration(time.Second).Milliseconds() * int64(i)
// 		delay := time.Duration(time.Now().UnixNano()).Milliseconds() + s
// 		RedisCli.ZAdd(context.Background(), "zadd_test", &redis.Z{
// 			Score:  float64(delay),
// 			Member: fmt.Sprintf("lilh-%d", i),
// 		})
// 	}

// }

// func TestZset(t *testing.T) {
// 	InitRedis()

// 	script := redis.NewScript(`
// 		local vals = redis.call("zrangebyscore",KEYS[1],"-inf",ARGV[1],"limit",0,20)
// 		if (next(vals) ~= nil) then
// 			redis.call("zremrangebyrank",KEYS[1], 0, #vals -1)
// 		end
// 		return #vals
// 	`)
// 	nums, err := script.Run(context.Background(), RedisCli, []string{"zadd1"}, time.Now().UnixNano()).Result()
// 	if err != nil {
// 		panic(err)
// 	}
// 	t.Log("nums: ", nums)
// }

func TestLuaIncrBy(t *testing.T) {
	config := RedisConfiguration{
		Host: "127.0.0.1",
		Port: "6379",
	}
	redisCli, err := initRedis(config)
	if err != nil {
		t.Log("initRedis Error: ", err)
	}

	keys := []string{"test1"}
	values := []interface{}{5}
	num, err := incrBy.Run(context.Background(), redisCli, keys, values...).Int()
	if err != nil {
		t.Log("Run Error: ", err)
	}
	t.Log("num: ", num)

}

// func TestLuaSum(t *testing.T) {
// 	InitRedis()
// 	keys := []string{"testsum"}
// 	values := []interface{}{1, 2, 3, 4}
// 	num, err := sum.Run(context.Background(), RedisCli, keys, values...).Int()
// 	if err != nil {
// 		panic(err)
// 	}
// 	t.Log("num: ", num)
// }

func TestRedisCall(t *testing.T) {
	config := RedisConfiguration{
		Host: "127.0.0.1",
		Port: "6379",
	}
	redisCli, err := initRedis(config)
	if err != nil {
		t.Log("initRedis Error: ", err)
	}
	topic := "topic_lilh"

	job := Job{
		ID:    uuid.NewV4().String(),
		Delay: 456545646,
		Topic: topic,
	}

	delayQueueKey := fmt.Sprintf("%s_%s", RedisDelayQueue, topic)
	jobPoolTopic := fmt.Sprintf("%s_%s", RedisJobPool, topic)
	jobBytes, _ := json.Marshal(job)
	fmt.Println("jobBytes: ", string(jobBytes))

	isSuccess, err := pushToDelayQueueScript.Run(context.Background(), redisCli, []string{delayQueueKey, jobPoolTopic},
		[]interface{}{job.ID, job.Delay, string(jobBytes)}).Bool()

	if err != nil {
		t.Log("Run Error: ", err)
	}
	t.Log("isSuccess: ", isSuccess)

}

var sum = redis.NewScript(`
	local key = KEYS[1]
	local sum = redis.call("GET",key)
	if not sum then 
		sum = 0
	end 

	local num_arg = #ARGV 
	for i = 1, num_arg do
		sum = sum + ARGV[i]
	end 

	redis.call("SET",key,sum)
	return sum
`)

var incrBy = redis.NewScript(`
	local key =  KEYS[1]
	local change = ARGV[1]

	local value = redis.call("GET",key)
	if not value then 
		value = 0 
	end 

	value = value + change 
	redis.call("SET",key,value)
	return value

`)
