-- KEYS: bucket state, optional LRU index (standalone Redis only).
-- ARGV: caller's time in seconds, rate, capacity, cost, TTL, max_keys (0 = unbounded).
-- Return 0 for Allow, otherwise the wait rounded UP to integer milliseconds.
local now = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])
local max_keys = tonumber(ARGV[6])

local tokens = capacity
local updated_at = now
local payload = redis.call('GET', KEYS[1])
if payload then
    local state = cjson.decode(payload)
    tokens = state.tokens
    updated_at = state.updated_at
    if type(tokens) ~= 'number' or type(updated_at) ~= 'number' then
        return redis.error_reply('ERR invalid token bucket state')
    end
end

-- Match TokenBucket's handling of backwards clocks and refill saturation.
local elapsed = math.max(0, now - updated_at)
tokens = math.min(capacity, tokens + elapsed * rate)
local wait_ms = 0
if tokens >= cost then
    tokens = tokens - cost
else
    wait_ms = math.ceil(((cost - tokens) / rate) * 1000)
end

-- cjson.encode defaults to fewer significant digits than a Python float.
-- Seventeen digits round-trip the bucket's double precision state.
local next_state = '{"tokens":' .. string.format('%.17g', tokens)
    .. ',"updated_at":' .. string.format('%.17g', now) .. '}'
redis.call('SET', KEYS[1], next_state, 'EX', ttl)

if max_keys > 0 then
    -- The sequence expresses access order even when FakeClock stands still.
    -- Expired buckets must not occupy the bounded LRU index.
    local entries = redis.call('ZRANGE', KEYS[2], 0, -1)
    for _, key in ipairs(entries) do
        if redis.call('EXISTS', key) == 0 then
            redis.call('ZREM', KEYS[2], key)
        end
    end
    local newest = redis.call('ZREVRANGE', KEYS[2], 0, 0, 'WITHSCORES')
    local sequence = #newest == 0 and 1 or tonumber(newest[2]) + 1
    redis.call('ZADD', KEYS[2], sequence, KEYS[1])
    while redis.call('ZCARD', KEYS[2]) > max_keys do
        local oldest = redis.call('ZRANGE', KEYS[2], 0, 0)[1]
        redis.call('DEL', oldest)
        redis.call('ZREM', KEYS[2], oldest)
    end
    redis.call('EXPIRE', KEYS[2], ttl)
end

return wait_ms
