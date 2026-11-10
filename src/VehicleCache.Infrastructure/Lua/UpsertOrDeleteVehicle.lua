-- UpsertOrDeleteVehicle.lua
-- Atomic conditional projection for `cdc.public.vehicles` events in Valkey.
-- KEYS[1] = vehicle:{vehicleId}
-- ARGV[1] = op ("r" | "c" | "u" | "d")
-- ARGV[2] = vehicleId
-- ARGV[3] = plate
-- ARGV[4] = state
-- ARGV[5] = updatedAtMs (int64 epoch ms)
-- ARGV[6] = lsn (int64 PostgreSQL WAL LSN)
--
-- Return codes:
--   1 = Applied
--   0 = SkippedDuplicate (idempotent no-op)
--  -1 = SkippedStale (out-of-order older event rejected)
--  -2 = SkippedTombstoned (event precedes or targets a deleted vehicle)

local key = KEYS[1]
local op = ARGV[1]
local vehicleId = ARGV[2]
local plate = ARGV[3]
local state = ARGV[4]
local incomingUpdatedAt = tonumber(ARGV[5])
local incomingLsn = tonumber(ARGV[6])

local exists = redis.call('EXISTS', key)
local curDeleted = redis.call('HGET', key, 'deleted') or '0'
local curDeletedLsn = tonumber(redis.call('HGET', key, 'deletedLsn') or '0')
local curStateUpdatedAt = tonumber(redis.call('HGET', key, 'stateUpdatedAt') or '0')
local curStateLsn = tonumber(redis.call('HGET', key, 'stateLsn') or '0')

if op == 'd' then
    if curDeleted == '1' and incomingLsn == curDeletedLsn then
        return 0 -- SkippedDuplicate
    end
    if incomingLsn < curDeletedLsn or incomingLsn < curStateLsn then
        return -1 -- SkippedStale
    end

    -- Canonical soft-tombstone state: guarantees 100% commutativity whether pre-delete
    -- positions/states arrived before or after this delete event during replay.
    redis.call('HSET', key,
        'vehicleId', vehicleId,
        'plate', '',
        'state', 'DELETED',
        'stateUpdatedAt', '0',
        'stateLsn', tostring(incomingLsn),
        'lat', '',
        'lng', '',
        'posRecordedAt', '0',
        'posLsn', '0',
        'deleted', '1',
        'deletedLsn', tostring(incomingLsn)
    )
    return 1 -- Applied
end

-- For op in {"r", "c", "u"}:
-- 1. Tombstone fence: never resurrect a vehicle from a pre-delete event
if curDeleted == '1' and incomingLsn <= curDeletedLsn then
    return -2 -- SkippedTombstoned
end

-- 2. Duplicate check (at-least-once delivery / snapshot re-emission)
if curStateLsn > 0 and incomingUpdatedAt == curStateUpdatedAt and incomingLsn == curStateLsn then
    return 0 -- SkippedDuplicate
end

-- 3. Monotonic state watermark check: (updatedAtMs, lsn)
local isNewer = (curStateLsn == 0)
    or (incomingUpdatedAt > curStateUpdatedAt)
    or (incomingUpdatedAt == curStateUpdatedAt and incomingLsn > curStateLsn)

if not isNewer then
    return -1 -- SkippedStale
end

if exists == 0 then
    redis.call('HSET', key,
        'vehicleId', vehicleId,
        'plate', plate,
        'state', state,
        'stateUpdatedAt', tostring(incomingUpdatedAt),
        'stateLsn', tostring(incomingLsn),
        'lat', '',
        'lng', '',
        'posRecordedAt', '0',
        'posLsn', '0',
        'deleted', '0',
        'deletedLsn', '0'
    )
else
    redis.call('HSET', key,
        'vehicleId', vehicleId,
        'plate', plate,
        'state', state,
        'stateUpdatedAt', tostring(incomingUpdatedAt),
        'stateLsn', tostring(incomingLsn),
        'deleted', '0'
    )
end

return 1 -- Applied
