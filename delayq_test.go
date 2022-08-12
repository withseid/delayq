package delayq

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func TestNewClient(t *testing.T) {
	config := RedisConfiguration{
		Host: "192.168.89.160",
		Port: "6379",
	}
	client := NewClient(config)
	a := []int{1, 2, 3}
	b, err := json.Marshal(a)
	if err != nil {
		panic(err)
	}
	client.Enqueue(b, "aaa", "job_id", ProcessAt(time.Now().AddDate(1, 0, 0)))

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

// func TestLuaIncrBy(t *testing.T) {
// 	InitRedis()

// 	keys := []string{"test1"}
// 	values := []interface{}{5}
// 	num, err := incrBy.Run(context.Background(), RedisCli, keys, values...).Int()
// 	if err != nil {
// 		panic(err)
// 	}
// 	t.Log("num: ", num)

// }

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
