package delayq

import "github.com/go-redis/redis/v8"

var unfinishedToReadyScript = redis.NewScript(`
	local processQueueKey = KEYS[1]
	local readyQueueKey = KEYS[2]
	local jobIDs = redis.call("smembers",processQueueKey)
	for key,value in ipairs(jobIDs) do 
		redis.call("lpush",readyQueueKey,value)
		redis.call("srem",processQueueKey,value)
	end 
	return jobIDs
`)

var migrateExpiredJobScript = redis.NewScript(`
	local delayQueueKey = KEYS[1]
	local readyQueueKey = KEYS[2]
	local t = ARGV[1]
	local jobIDs = redis.call("zrangebyscore",delayQueueKey,0,t)
	for key,value in ipairs(jobIDs) do
		redis.call('lpush',readyQueueKey,value)
		redis.call('zrem',delayQueueKey,value)
	end
	return jobIDs
`)

var pushToDelayQueueScript = redis.NewScript(`
	local delayQueueKey = KEYS[1]
	local jobPoolKey = KEYS[2]
	local processQueueKey = KEYS[3]
	local jobID = ARGV[1]
	local delay = ARGV[2]
	local payload = ARGV[3]
	local result = redis.call("zadd",delayQueueKey,delay,jobID)
	if type(result) == "table" and result.err then 
		return false
	end 

	local result = redis.call('hset',jobPoolKey,jobID,payload)
	if type(result) == "table" and result.err then 
		return false 
	end

	redis.call("srem",processQueueKey,jobID)

	return true 
`)

var pushToReadyQueueScript = redis.NewScript(`
	local readyQueueKey = KEYS[1]
	local jobPoolKey = KEYS[2]
	local jobID = ARGV[1]
	local payload = ARGV[2]
	local result = redis.call("lpush",readyQueueKey,jobID)
	if type(result) == "table" and result.err then 
		return false 
	end 

	local result = redis.call("hset",jobPoolKey,jobID, payload)
	if type(result) == "table" and result.err then 
		return false 
	end 

	return true

`)

var getReadyJobScript = redis.NewScript(`
	local readyQueueKey = KEYS[1]
	local processQueueKey = KEYS[2]
	local jobPoolKey = KEYS[3]
	
	local jobID = redis.call("rpop",readyQueueKey)
	redis.call("sadd",processQueueKey,jobID)
	local job = redis.call("hget",jobPoolKey,jobID)
	return job
`)

var deleteDelayJobScript = redis.NewScript(`
	local delayQueueKey = KEYS[1]
	local jobPoolKey = KEYS[2]
	local jobID = ARGV[1]

	redis.call("zrem",delayQueueKey,jobID)
	redis.call("hdel",jobPoolKey,jobID)

	return true
`)

var deleteFinishJobScript = redis.NewScript(`
	local processQueueKey = KEYS[1]
	local jobPoolKey = KEYS[2]
	local jobID = ARGV[1]
	redis.call("srem",processQueueKey,jobID)
	redis.call("hdel",jobPoolKey,jobID)
	return true 
`)
