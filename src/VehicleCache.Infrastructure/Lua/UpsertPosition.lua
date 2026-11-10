-- UpsertPosition.lua
-- Atomic conditional projection for `cdc.public.positions` events in Valkey.
-- KEYS[1] = vehicle:{vehicleId}
-- ARGV[1] = vehicleId
-- ARGV[2] = lat (invariant string)
-- ARGV[3] = lng (invariant string)
-- ARGV[4] = recordedAtMs (int64 epoch ms)
-- ARGV[5] = lsn (int64 PostgreSQL WAL LSN)
--
-- Return codes:
--   1 = Applied
--   0 = SkippedDuplicate (idempotent no-op)
--  -1 = SkippedStale (out-of-order older position rejected)
--  -2 = SkippedTombstoned (position precedes or targets a deleted vehicle)

local key = KEYS[1]
local vehicleId = ARGV[1]
local lat = ARGV[2]
local lng = ARGV[3]
local incomingRecordedAt = tonumber(ARGV[4])
local incomingLsn = tonumber(ARGV[5])

local exists = redis.call('EXISTS', key)
local curDeleted = redis.call('HGET', key, 'deleted') or '0'
local curDeletedLsn = tonumber(redis.call('HGET', key, 'deletedLsn') or '0')
local curPosRecordedAt = tonumber(redis.call('HGET', key, 'posRecordedAt') or '0')
local curPosLsn = tonumber(redis.call('HGET', key, 'posLsn') or '0')

-- 1. Tombstone fence: never resurrect a deleted vehicle from a late position event
if curDeleted == '1' or (curDeletedLsn > 0 and incomingLsn <= curDeletedLsn) then
    return -2 -- SkippedTombstoned
end

-- 2. Duplicate check (at-least-once delivery / replay)
if curPosLsn > 0 and incomingRecordedAt == curPosRecordedAt and incomingLsn == curPosLsn then
    return 0 -- SkippedDuplicate
end

-- 3. Monotonic position watermark check: (recordedAtMs, lsn)
local isNewer = (curPosLsn == 0)
    or (incomingRecordedAt > curPosRecordedAt)
    or (incomingRecordedAt == curPosRecordedAt and incomingLsn > curPosLsn)

if not isNewer then
    return -1 -- SkippedStale
end

if exists == 0 then
    -- Cross-stream race: position arrived before vehicle row CDC event.
    -- Initialize partial hash with state = "UNKNOWN" so subsequent vehicle CDC merges cleanly.
    redis.call('HSET', key,
        'vehicleId', vehicleId,
        'plate', '',
        'state', 'UNKNOWN',
        'stateUpdatedAt', '0',
        'stateLsn', '0',
        'lat', lat,
        'lng', lng,
        'posRecordedAt', tostring(incomingRecordedAt),
        'posLsn', tostring(incomingLsn),
        'deleted', '0',
        'deletedLsn', '0'
    )
else
    redis.call('HSET', key,
        'vehicleId', vehicleId,
        'lat', lat,
        'lng', lng,
        'posRecordedAt', tostring(incomingRecordedAt),
        'posLsn', tostring(incomingLsn)
    )
end

return 1 -- Applied
