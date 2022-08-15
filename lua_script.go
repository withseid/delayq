package delayq

import "github.com/go-redis/redis/v8"

var pushToDelayQueueScript = redis.NewScript(`
	local delayQueueKey = KEYS[1]
	local jobPoolKey = KEYS[2]
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
	local jobPoolKey = KEYS[2]
	
	local jobID = redis.pcall("rpop",readyQueueKey)

	local job = redis.call("hget",jobPoolKey,jobID)

	redis.call("hdel",jobPoolKey,jobID)

	return job
`)
