"""Production Redis Lua Scripts for Distributed Rate Limiting.

System Design Analysis:
In a multi-node distributed architecture (e.g., 50 API Gateway nodes behind a load balancer),
local memory rate limiters cannot enforce a shared global quota.

Naive approaches (GET count -> check -> INCR count) fail due to distributed race conditions.
Distributed locks (SETNX / Redlock) introduce heavy network latency and lock contention.

Industry Standard Solution: Redis Lua Scripts.
Because Redis executes Lua scripts atomically in a single thread, the check-and-decrement
is guaranteed atomic with 0 lock overhead and sub-millisecond network turnaround.
"""

# 1. Distributed Token Bucket in Redis Lua
REDIS_TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

-- Hash fields: 'tokens' and 'last_refill'
local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])

if tokens == nil then
    tokens = capacity
    last_refill = now
else
    local elapsed = math.max(0, now - last_refill)
    tokens = math.min(capacity, tokens + (elapsed * refill_rate))
    last_refill = now
end

if tokens >= cost then
    tokens = tokens - cost
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', last_refill)
    -- Expire key when full bucket would be idle
    local ttl = math.ceil(capacity / refill_rate) * 2
    redis.call('EXPIRE', key, ttl)
    return {1, math.floor(tokens)} -- Allowed
else
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', last_refill)
    return {0, math.floor(tokens)} -- Rejected (429)
end
"""

# 2. Distributed Sliding Window Log in Redis Lua (Sorted Set / ZSET)
REDIS_SLIDING_WINDOW_LOG_LUA = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local clear_before = now - window

-- 1. Remove expired timestamps outside the rolling window
redis.call('ZREMRANGEBYSCORE', key, '-inf', clear_before)

-- 2. Check current count in window
local current_count = redis.call('ZCARD', key)

if current_count + cost <= limit then
    -- Add unique request entries
    for i = 1, cost do
        local member = tostring(now) .. '-' .. tostring(i) .. '-' .. tostring(math.random(1000, 9999))
        redis.call('ZADD', key, now, member)
    end
    redis.call('EXPIRE', key, math.ceil(window))
    return {1, limit - (current_count + cost)}
else
    return {0, math.max(0, limit - current_count)}
end
"""

# 3. Distributed Sliding Window Counter in Redis Lua
REDIS_SLIDING_WINDOW_COUNTER_LUA = """
local base_key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_size = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])

local current_window = math.floor(now / window_size)
local prev_window = current_window - 1

local curr_key = base_key .. ':' .. current_window
local prev_key = base_key .. ':' .. prev_window

local curr_count = tonumber(redis.call('GET', curr_key) or '0')
local prev_count = tonumber(redis.call('GET', prev_key) or '0')

local time_in_curr_window = now - (current_window * window_size)
local progress = time_in_curr_window / window_size
local overlap_weight = 1.0 - progress

local estimated_requests = (prev_count * overlap_weight) + curr_count

if estimated_requests + cost <= limit then
    redis.call('INCRBY', curr_key, cost)
    redis.call('EXPIRE', curr_key, math.ceil(window_size * 2))
    local remaining = math.max(0, math.floor(limit - (estimated_requests + cost)))
    return {1, remaining}
else
    local remaining = math.max(0, math.floor(limit - estimated_requests))
    return {0, remaining}
end
"""
