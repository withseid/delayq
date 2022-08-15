package delayq

import "github.com/go-redis/redis/v8"

var pushToDelayQueueScript = redis.NewScript(`
	local delayQueueKey = KEYS[1]
	local jobPoolKey = KEYS[2]
	local jobID = ARGV[1]
	local delay = ARGV[2]
	local payload = ARGV[3]
	local result = redis.pcall("zadd",delayQueueKey,delay,jobID)
	if type(result) == "table" and result.err then 
		return false
	end 

	local result = redis.pcall('hset',jobPoolKey,jobID,payload)
	if type(result) == "table" and result.err then 
		return false 
	end

	return true 
`)

var pushToReadyQueue = redis.NewScript(`
	local readyQueueKey = KEYS[1]
	local jobPoolKey = KEYS[2]
	local jobID = ARGV[1]
	local payload = ARGV[2]
	local result = redis.pcall("lpush",readyQueueKey,jobID)
	if type(result) == "table" and result.err then 
		return false 
	end 

	local result = redis.pcall("hset",jobPoolKey,jobID, payload)
	if type(result) == "table" and result.err then 
		return false 
	end 

	return true

`)
