"""具名 Redis Lua 脚本；调用方的 key/arg 布局由相邻测试固定。"""

BATCH_SUBMIT = """
local count = tonumber(ARGV[1]); local output = {}
for index = 0, count - 1 do
  local key = index * 8; local arg = 2 + index * 15
  local duplicate = false
  if ARGV[arg] ~= '' then
    local old = redis.call('GET', KEYS[key + 1])
    if old then
      table.insert(output, 0); table.insert(output, old); table.insert(output, redis.call('PTTL', KEYS[key + 1])); table.insert(output, '')
      duplicate = true
    else
      redis.call('SET', KEYS[key + 1], ARGV[arg + 1], 'PX', ARGV[arg + 2])
    end
  end
  if not duplicate then
    local entry = ''
    if ARGV[arg + 5] == 'ready' then
      entry = redis.call('XADD', KEYS[key + 3], '*', 'message_id', ARGV[arg + 1], 'envelope', ARGV[arg + 3])
      redis.call('ZADD', KEYS[key + 7], ARGV[arg + 11], ARGV[arg + 1])
    end
    redis.call('HSET', KEYS[key + 2], 'envelope', ARGV[arg + 3], 'queue', ARGV[arg + 4], 'status', ARGV[arg + 5], 'entry_id', entry, 'attempt', '0', 'max_attempts', ARGV[arg + 6], 'created_at', ARGV[arg + 7], 'expires_at', ARGV[arg + 8], 'serializer_name', ARGV[arg + 9], 'serializer_version', ARGV[arg + 10], 'available_at', ARGV[arg + 12], 'workflow_id', ARGV[arg + 13], 'parent_id', ARGV[arg + 14])
    if ARGV[arg + 5] == 'delayed' then redis.call('ZADD', KEYS[key + 8], ARGV[arg + 12], ARGV[arg + 1]) end
    if ARGV[arg + 5] == 'expired' then redis.call('HSET', KEYS[key + 2], 'expired_at', ARGV[arg + 7], 'status_at_expiry', 'ready', 'last_delivery_id', ''); redis.call('LPUSH', KEYS[key + 5], ARGV[arg + 1]) end
    if (ARGV[arg + 5] == 'ready' or ARGV[arg + 5] == 'delayed') and ARGV[arg + 8] ~= '0' then redis.call('ZADD', KEYS[key + 4], ARGV[arg + 8], ARGV[arg + 1]) end
    redis.call('HINCRBY', KEYS[key + 6], 'submitted_total', 1)
    table.insert(output, 1); table.insert(output, ARGV[arg + 1]); table.insert(output, ARGV[arg + 2]); table.insert(output, entry)
  end
end
return output
"""

# Replay scripts move the message hash to the target queue and update the global
# lookup index. They require one source and one target queue keyspace.
REPLAY_DEAD_LETTER = """
if ARGV[7] == '0' and KEYS[7] ~= '' then local current = redis.call('GET', KEYS[7]); if current and current ~= ARGV[1] then return -1 end end
local fields = redis.call('HGETALL', KEYS[1])
if #fields == 0 or redis.call('LREM', KEYS[2], 1, ARGV[1]) == 0 then return 0 end
if ARGV[7] == '0' then if KEYS[6] ~= '' and redis.call('GET', KEYS[6]) == ARGV[1] then redis.call('DEL', KEYS[6]) end; if KEYS[7] ~= '' then redis.call('SET', KEYS[7], ARGV[1], 'PX', ARGV[8]) end end
if KEYS[8] ~= KEYS[1] then redis.call('HSET', KEYS[8], unpack(fields)); redis.call('DEL', KEYS[1]) end
local entry = redis.call('XADD', KEYS[3], '*', 'message_id', ARGV[1], 'envelope', ARGV[2])
redis.call('HSET', KEYS[8], 'envelope', ARGV[2], 'queue', ARGV[3], 'status', 'ready', 'entry_id', entry, 'attempt', ARGV[4], 'last_action', 'replayed')
redis.call('HSET', KEYS[9], ARGV[1], ARGV[3])
if ARGV[5] ~= '0' then redis.call('ZADD', KEYS[4], ARGV[5], ARGV[1]) end
redis.call('ZADD', KEYS[5], ARGV[6], ARGV[1]); return 1
"""

REPLAY_EXPIRED = """
if ARGV[5] == '0' and KEYS[7] ~= '' then local current = redis.call('GET', KEYS[7]); if current and current ~= ARGV[1] then return -1 end end
local fields = redis.call('HGETALL', KEYS[1])
if #fields == 0 or redis.call('LREM', KEYS[2], 1, ARGV[1]) == 0 then return 0 end
if ARGV[5] == '0' then if KEYS[6] ~= '' and redis.call('GET', KEYS[6]) == ARGV[1] then redis.call('DEL', KEYS[6]) end; if KEYS[7] ~= '' then redis.call('SET', KEYS[7], ARGV[1], 'PX', ARGV[6]) end end
if KEYS[8] ~= KEYS[1] then redis.call('HSET', KEYS[8], unpack(fields)); redis.call('DEL', KEYS[1]) end
local entry = redis.call('XADD', KEYS[3], '*', 'message_id', ARGV[1], 'envelope', ARGV[2])
redis.call('HSET', KEYS[8], 'envelope', ARGV[2], 'queue', ARGV[7], 'status', 'ready', 'entry_id', entry, 'expires_at', ARGV[3], 'last_action', 'replayed')
redis.call('HSET', KEYS[9], ARGV[1], ARGV[7])
if ARGV[3] ~= '0' then redis.call('ZADD', KEYS[4], ARGV[3], ARGV[1]) end
redis.call('ZADD', KEYS[5], ARGV[4], ARGV[1]); return 1
"""

PEL_RECOVER = """
if redis.call('HGET', KEYS[1], 'status') ~= 'ready' or redis.call('HGET', KEYS[1], 'entry_id') ~= ARGV[2] then return 0 end
redis.call('XACK', KEYS[2], ARGV[1], ARGV[2]); redis.call('XDEL', KEYS[2], ARGV[2])
local next_entry = redis.call('XADD', KEYS[2], '*', 'message_id', ARGV[3], 'envelope', redis.call('HGET', KEYS[1], 'envelope'))
redis.call('HSET', KEYS[1], 'entry_id', next_entry, 'last_action', 'pel_recovered')
redis.call('HINCRBY', KEYS[3], 'reclaimed_total', 1); return 1
"""

CLAIM = """
local state = redis.call('HGET', KEYS[1], 'status')
if state ~= 'ready' or redis.call('HGET', KEYS[1], 'entry_id') ~= ARGV[8] then redis.call('XACK', KEYS[5], ARGV[7], ARGV[8]); redis.call('XDEL', KEYS[5], ARGV[8]); return 0 end
local expires = tonumber(redis.call('HGET', KEYS[1], 'expires_at') or '0')
if expires > 0 and expires <= tonumber(ARGV[1]) then
  redis.call('HSET', KEYS[1], 'status', 'expired', 'last_action', 'expired', 'expired_at', ARGV[1], 'status_at_expiry', 'ready')
  redis.call('XACK', KEYS[5], ARGV[7], ARGV[8]); redis.call('XDEL', KEYS[5], ARGV[8]); redis.call('LPUSH', KEYS[3], ARGV[2]); redis.call('ZREM', KEYS[4], ARGV[2]); redis.call('ZREM', KEYS[6], ARGV[2]); return -1
end
local attempt = redis.call('HINCRBY', KEYS[1], 'attempt', 1)
redis.call('HSET', KEYS[1], 'status', 'leased', 'consumer_id', ARGV[3], 'delivery_id', ARGV[4], 'lease_token', ARGV[5], 'claimed_at', ARGV[1], 'lease_until', ARGV[6], 'last_action', '', 'entry_id', ARGV[8])
redis.call('ZADD', KEYS[2], ARGV[6], ARGV[2]); redis.call('ZREM', KEYS[6], ARGV[2]); return attempt
"""

EXTEND_LEASE = """
local time = redis.call('TIME'); local now = tonumber(time[1]) + tonumber(time[2]) / 1000000
if redis.call('HGET', KEYS[1], 'status') ~= 'leased' or redis.call('HGET', KEYS[1], 'delivery_id') ~= ARGV[1] or redis.call('HGET', KEYS[1], 'lease_token') ~= ARGV[2] or tonumber(redis.call('HGET', KEYS[1], 'lease_until') or '0') <= now then return 0 end
local expires = tonumber(redis.call('HGET', KEYS[1], 'expires_at') or '0')
if expires > 0 and expires <= now then
  local entry = redis.call('HGET', KEYS[1], 'entry_id'); redis.call('XACK', KEYS[3], ARGV[5], entry); redis.call('XDEL', KEYS[3], entry)
  redis.call('HSET', KEYS[1], 'status', 'expired', 'last_action', 'expired', 'expired_at', now, 'status_at_expiry', 'leased', 'last_delivery_id', ARGV[1]); redis.call('HDEL', KEYS[1], 'consumer_id', 'delivery_id', 'lease_token', 'claimed_at', 'lease_until')
  redis.call('ZREM', KEYS[2], ARGV[4]); redis.call('ZREM', KEYS[4], ARGV[4]); redis.call('LPUSH', KEYS[5], ARGV[4]); return -1
end
redis.call('HSET', KEYS[1], 'lease_until', ARGV[3]); redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4]); return 1
"""

# KEYS: message, leases, expiry, stream, eq, stats, dlq, ready, delayed, retention.
# ARGV: action, delivery_id, token, now, message_id, reason, error_type, group,
# retry_available_at, requested_max_attempts, ack_tombstone_ttl.
FINISH = """
local status = redis.call('HGET', KEYS[1], 'status'); local current = redis.call('HGET', KEYS[1], 'delivery_id')
if status ~= 'leased' then if redis.call('HGET', KEYS[1], 'last_delivery_id') == ARGV[2] and redis.call('HGET', KEYS[1], 'last_action') == ARGV[1] then return 2 end; return 0 end
if current ~= ARGV[2] or redis.call('HGET', KEYS[1], 'lease_token') ~= ARGV[3] or tonumber(redis.call('HGET', KEYS[1], 'lease_until') or '0') <= tonumber(ARGV[4]) then return 0 end
local entry = redis.call('HGET', KEYS[1], 'entry_id'); local expires = tonumber(redis.call('HGET', KEYS[1], 'expires_at') or '0')
if expires > 0 and expires <= tonumber(ARGV[4]) then
  redis.call('HSET', KEYS[1], 'status', 'expired', 'last_action', 'expired', 'expired_at', ARGV[4], 'status_at_expiry', 'leased', 'last_delivery_id', ARGV[2]); redis.call('HDEL', KEYS[1], 'consumer_id', 'delivery_id', 'lease_token', 'claimed_at', 'lease_until')
  redis.call('XACK', KEYS[4], ARGV[8], entry); redis.call('XDEL', KEYS[4], entry); redis.call('ZREM', KEYS[2], ARGV[5]); redis.call('ZREM', KEYS[3], ARGV[5]); redis.call('ZREM', KEYS[8], ARGV[5]); redis.call('ZREM', KEYS[9], ARGV[5]); redis.call('LPUSH', KEYS[5], ARGV[5]); return 3
end
local attempt = tonumber(redis.call('HGET', KEYS[1], 'attempt')); local stored_max = tonumber(redis.call('HGET', KEYS[1], 'max_attempts')); local requested_max = tonumber(ARGV[10]); local max_attempts = (requested_max > 0 and math.min(stored_max, requested_max)) or stored_max
redis.call('ZREM', KEYS[2], ARGV[5])
if ARGV[1] == 'ack' then
  redis.call('XACK', KEYS[4], ARGV[8], entry); redis.call('XDEL', KEYS[4], entry); local retention_until = tonumber(ARGV[4]) + tonumber(ARGV[11]); redis.call('HSET', KEYS[1], 'status', 'acked', 'last_action', 'ack', 'last_delivery_id', ARGV[2], 'acked_at', ARGV[4], 'retention_until', retention_until); redis.call('HDEL', KEYS[1], 'consumer_id', 'delivery_id', 'lease_token', 'claimed_at', 'lease_until'); redis.call('ZREM', KEYS[3], ARGV[5]); redis.call('ZADD', KEYS[10], retention_until, ARGV[5]); redis.call('HINCRBY', KEYS[6], 'acked_total', 1); return 4
elseif ARGV[1] == 'retry' and attempt < max_attempts then
  redis.call('XACK', KEYS[4], ARGV[8], entry); redis.call('XDEL', KEYS[4], entry); local next_status = 'ready'; local new_entry = ''
  if tonumber(ARGV[9]) > 0 then next_status = 'delayed'; redis.call('ZADD', KEYS[9], ARGV[9], ARGV[5]) else new_entry = redis.call('XADD', KEYS[4], '*', 'message_id', ARGV[5], 'envelope', redis.call('HGET', KEYS[1], 'envelope')); redis.call('ZADD', KEYS[8], ARGV[4], ARGV[5]) end
  redis.call('HSET', KEYS[1], 'status', next_status, 'entry_id', new_entry, 'available_at', ARGV[9], 'last_action', 'retry', 'last_reason', ARGV[6], 'last_delivery_id', ARGV[2]); redis.call('HDEL', KEYS[1], 'consumer_id', 'delivery_id', 'lease_token', 'claimed_at', 'lease_until'); redis.call('HINCRBY', KEYS[6], 'retried_total', 1); return 5
else
  local source = ARGV[1] == 'reject' and 'reject' or 'retry_limit'; redis.call('XACK', KEYS[4], ARGV[8], entry); redis.call('XDEL', KEYS[4], entry); redis.call('HSET', KEYS[1], 'status', 'dead_lettered', 'last_action', ARGV[1], 'last_reason', ARGV[6], 'dead_source', source, 'failed_at', ARGV[4], 'error_type', ARGV[7], 'last_delivery_id', ARGV[2]); redis.call('HDEL', KEYS[1], 'consumer_id', 'delivery_id', 'lease_token', 'claimed_at', 'lease_until'); redis.call('ZREM', KEYS[3], ARGV[5]); redis.call('LPUSH', KEYS[7], ARGV[5]); redis.call('HINCRBY', KEYS[6], 'dead_lettered_total', 1); return 6
end
"""

# Maintenance scripts are invoked for one message at a time.  Their keys and
# argv order are fixed at RedisBroker.maintain and intentionally kept here.
DUE_DELAYED = """
if redis.call('HGET', KEYS[1], 'status') ~= 'delayed' then return 0 end
local available = tonumber(redis.call('HGET', KEYS[1], 'available_at') or '0'); if available == 0 or available > tonumber(ARGV[1]) then return 0 end
local expires = tonumber(redis.call('HGET', KEYS[1], 'expires_at') or '0')
if expires > 0 and expires <= tonumber(ARGV[1]) then
  redis.call('HSET', KEYS[1], 'status', 'expired', 'last_action', 'expired', 'expired_at', ARGV[1], 'status_at_expiry', 'delayed', 'last_delivery_id', ''); redis.call('ZREM', KEYS[2], ARGV[2]); redis.call('ZREM', KEYS[3], ARGV[2]); redis.call('ZREM', KEYS[6], ARGV[2]); redis.call('LPUSH', KEYS[5], ARGV[2]); return 2
end
local entry = redis.call('XADD', KEYS[4], '*', 'message_id', ARGV[2], 'envelope', redis.call('HGET', KEYS[1], 'envelope'))
redis.call('HSET', KEYS[1], 'status', 'ready', 'entry_id', entry, 'available_at', '', 'last_action', 'due'); redis.call('ZREM', KEYS[2], ARGV[2]); redis.call('ZADD', KEYS[3], ARGV[1], ARGV[2]); return 1
"""

EXPIRE = """
local status = redis.call('HGET', KEYS[1], 'status'); local expires = tonumber(redis.call('HGET', KEYS[1], 'expires_at') or '0')
if (status ~= 'ready' and status ~= 'leased' and status ~= 'delayed') or expires == 0 or expires > tonumber(ARGV[1]) then return 0 end
local entry = redis.call('HGET', KEYS[1], 'entry_id'); if entry and entry ~= '' then redis.call('XACK', KEYS[4], ARGV[2], entry); redis.call('XDEL', KEYS[4], entry) end
redis.call('HSET', KEYS[1], 'status', 'expired', 'last_action', 'expired', 'expired_at', ARGV[1], 'status_at_expiry', status, 'last_delivery_id', redis.call('HGET', KEYS[1], 'delivery_id') or ''); redis.call('HDEL', KEYS[1], 'consumer_id', 'delivery_id', 'lease_token', 'claimed_at', 'lease_until')
redis.call('ZREM', KEYS[2], ARGV[3]); redis.call('ZREM', KEYS[3], ARGV[3]); redis.call('ZREM', KEYS[6], ARGV[3]); redis.call('ZREM', KEYS[7], ARGV[3]); redis.call('LPUSH', KEYS[5], ARGV[3]); return 1
"""

RECLAIM_LEASE = """
if redis.call('HGET', KEYS[1], 'status') ~= 'leased' or tonumber(redis.call('HGET', KEYS[1], 'lease_until') or '0') > tonumber(ARGV[1]) then return 0 end
local entry = redis.call('HGET', KEYS[1], 'entry_id'); local expires = tonumber(redis.call('HGET', KEYS[1], 'expires_at') or '0'); redis.call('XACK', KEYS[4], ARGV[2], entry); redis.call('XDEL', KEYS[4], entry); redis.call('ZREM', KEYS[2], ARGV[3])
if expires > 0 and expires <= tonumber(ARGV[1]) then
  redis.call('HSET', KEYS[1], 'status', 'expired', 'last_action', 'expired', 'expired_at', ARGV[1], 'status_at_expiry', 'leased', 'last_delivery_id', redis.call('HGET', KEYS[1], 'delivery_id') or ''); redis.call('HDEL', KEYS[1], 'consumer_id', 'delivery_id', 'lease_token', 'claimed_at', 'lease_until'); redis.call('ZREM', KEYS[3], ARGV[3]); redis.call('LPUSH', KEYS[7], ARGV[3]); return 3
end
local attempt = tonumber(redis.call('HGET', KEYS[1], 'attempt')); local maximum = tonumber(redis.call('HGET', KEYS[1], 'max_attempts'))
if attempt >= maximum then
  redis.call('HSET', KEYS[1], 'status', 'dead_lettered', 'dead_source', 'lease_timeout', 'last_action', 'lease_timeout', 'last_delivery_id', redis.call('HGET', KEYS[1], 'delivery_id') or ''); redis.call('HDEL', KEYS[1], 'consumer_id', 'delivery_id', 'lease_token', 'claimed_at', 'lease_until'); redis.call('ZREM', KEYS[3], ARGV[3]); redis.call('LPUSH', KEYS[5], ARGV[3]); redis.call('HINCRBY', KEYS[6], 'dead_lettered_total', 1); return 2
else
  local next_entry = redis.call('XADD', KEYS[4], '*', 'message_id', ARGV[3], 'envelope', redis.call('HGET', KEYS[1], 'envelope')); redis.call('HSET', KEYS[1], 'status', 'ready', 'entry_id', next_entry, 'last_action', 'reclaimed', 'last_delivery_id', redis.call('HGET', KEYS[1], 'delivery_id') or ''); redis.call('HDEL', KEYS[1], 'consumer_id', 'delivery_id', 'lease_token', 'claimed_at', 'lease_until'); redis.call('ZADD', KEYS[8], ARGV[1], ARGV[3]); redis.call('HINCRBY', KEYS[6], 'reclaimed_total', 1)
end
return 1
"""

# Prune the business envelope of a due ACKED message while retaining its
# lightweight operational tombstone and global lookup index.
CLEANUP_ACKED = """
local status = redis.call('HGET', KEYS[1], 'status')
if not status then redis.call('ZREM', KEYS[2], ARGV[2]); return 0 end
if status ~= 'acked' then redis.call('ZREM', KEYS[2], ARGV[2]); return 0 end
local retention_until = tonumber(redis.call('HGET', KEYS[1], 'retention_until') or '0')
if retention_until == 0 or retention_until > tonumber(ARGV[1]) then return 0 end
redis.call('HDEL', KEYS[1], 'envelope'); redis.call('HSET', KEYS[1], 'payload_pruned', '1'); redis.call('ZREM', KEYS[2], ARGV[2]); return 1
"""

# Single submit: eight keys; arguments mirror RedisSubmissionStore.submit.
SUBMIT = """
if ARGV[1] ~= '' then local old = redis.call('GET', KEYS[1]); if old then return {0, old, redis.call('PTTL', KEYS[1])} end; redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3]) end
local entry = ''
if ARGV[6] == 'ready' then entry = redis.call('XADD', KEYS[3], '*', 'message_id', ARGV[2], 'envelope', ARGV[4]); redis.call('ZADD', KEYS[7], ARGV[12], ARGV[2]) end
redis.call('HSET', KEYS[2], 'envelope', ARGV[4], 'queue', ARGV[5], 'status', ARGV[6], 'entry_id', entry, 'attempt', '0', 'max_attempts', ARGV[7], 'created_at', ARGV[8], 'expires_at', ARGV[9], 'available_at', ARGV[13], 'serializer_name', ARGV[10], 'serializer_version', ARGV[11], 'workflow_id', ARGV[14], 'parent_id', ARGV[15])
if ARGV[6] == 'delayed' then redis.call('ZADD', KEYS[8], ARGV[13], ARGV[2]) end
if ARGV[6] == 'expired' then redis.call('HSET', KEYS[2], 'expired_at', ARGV[8], 'status_at_expiry', 'ready', 'last_delivery_id', ''); redis.call('LPUSH', KEYS[5], ARGV[2]) end
if (ARGV[6] == 'ready' or ARGV[6] == 'delayed') and ARGV[9] ~= '0' then redis.call('ZADD', KEYS[4], ARGV[9], ARGV[2]) end
redis.call('HINCRBY', KEYS[6], 'submitted_total', 1); return {1, ARGV[2], ARGV[3], entry}
"""
