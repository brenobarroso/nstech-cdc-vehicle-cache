using System.Globalization;
using System.Reflection;
using StackExchange.Redis;
using VehicleCache.Core.Abstractions;
using VehicleCache.Core.Domain;

namespace VehicleCache.Infrastructure.Projection;

/// <summary>
/// Projects Debezium CDC events into Valkey using atomic server-side Lua scripts (<c>EVAL</c>/<c>EVALSHA</c>).
/// Guarantees single-round-trip conditional updates and soft-tombstone protection against out-of-order resurrection.
/// </summary>
public sealed class ValkeyLuaProjector : IVehicleReadModelProjector
{
    private readonly IConnectionMultiplexer _multiplexer;
    private readonly string _vehicleLuaScript;
    private readonly string _positionLuaScript;

    public ValkeyLuaProjector(IConnectionMultiplexer multiplexer)
    {
        _multiplexer = multiplexer ?? throw new ArgumentNullException(nameof(multiplexer));
        _vehicleLuaScript = LoadEmbeddedLuaScript("UpsertOrDeleteVehicle.lua");
        _positionLuaScript = LoadEmbeddedLuaScript("UpsertPosition.lua");
    }

    public async Task<ProjectionOutcome> ProjectVehicleAsync(VehicleCdcEvent evt, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(evt);
        ct.ThrowIfCancellationRequested();

        var db = _multiplexer.GetDatabase();
        var key = (RedisKey)VehicleReadModel.GetKey(evt.VehicleId);

        var result = (int)await db.ScriptEvaluateAsync(
            _vehicleLuaScript,
            keys: [key],
            values:
            [
                evt.Operation.ToLowerInvariant(),
                evt.VehicleId.ToString("D"),
                evt.Plate ?? string.Empty,
                evt.State ?? VehicleStates.Registered,
                evt.UpdatedAtUnixMs.ToString(CultureInfo.InvariantCulture),
                evt.Lsn.ToString(CultureInfo.InvariantCulture)
            ]).ConfigureAwait(false);

        return (ProjectionOutcome)result;
    }

    public async Task<ProjectionOutcome> ProjectPositionAsync(PositionCdcEvent evt, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(evt);
        ct.ThrowIfCancellationRequested();

        var db = _multiplexer.GetDatabase();
        var key = (RedisKey)VehicleReadModel.GetKey(evt.VehicleId);

        var result = (int)await db.ScriptEvaluateAsync(
            _positionLuaScript,
            keys: [key],
            values:
            [
                evt.VehicleId.ToString("D"),
                evt.Latitude.ToString("G17", CultureInfo.InvariantCulture),
                evt.Longitude.ToString("G17", CultureInfo.InvariantCulture),
                evt.RecordedAtUnixMs.ToString(CultureInfo.InvariantCulture),
                evt.Lsn.ToString(CultureInfo.InvariantCulture)
            ]).ConfigureAwait(false);

        return (ProjectionOutcome)result;
    }

    public async Task<VehicleReadModel?> GetVehicleAsync(Guid vehicleId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        var db = _multiplexer.GetDatabase();
        var entries = await db.HashGetAllAsync(VehicleReadModel.GetKey(vehicleId)).ConfigureAwait(false);
        if (entries.Length == 0)
        {
            return null;
        }

        return MapHashEntries(vehicleId, entries);
    }

    public async Task<IReadOnlyDictionary<Guid, VehicleReadModel>> SnapshotAllAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        var db = _multiplexer.GetDatabase();
        var endpoints = _multiplexer.GetEndPoints();
        var server = _multiplexer.GetServer(endpoints[0]);

        var result = new Dictionary<Guid, VehicleReadModel>();
        await foreach (var key in server.KeysAsync(pattern: "vehicle:*").WithCancellation(ct).ConfigureAwait(false))
        {
            var keyStr = key.ToString();
            if (!keyStr.StartsWith("vehicle:", StringComparison.Ordinal) ||
                !Guid.TryParse(keyStr.AsSpan("vehicle:".Length), out var vehicleId))
            {
                continue;
            }

            var entries = await db.HashGetAllAsync(key).ConfigureAwait(false);
            if (entries.Length > 0)
            {
                result[vehicleId] = MapHashEntries(vehicleId, entries);
            }
        }

        return result;
    }

    public async Task FlushAllAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        var db = _multiplexer.GetDatabase();
        await db.ExecuteAsync("FLUSHALL").ConfigureAwait(false);
    }

    private static VehicleReadModel MapHashEntries(Guid fallbackId, HashEntry[] entries)
    {
        var map = entries.ToDictionary(
            e => e.Name.ToString(),
            e => e.Value.ToString(),
            StringComparer.Ordinal);

        var id = map.TryGetValue("vehicleId", out var idStr) && Guid.TryParse(idStr, out var parsedId)
            ? parsedId
            : fallbackId;
        var plate = map.GetValueOrDefault("plate", string.Empty);
        var state = map.GetValueOrDefault("state", VehicleStates.Unknown);
        var stateUpdatedAt = ParseLongOrDefault(map.GetValueOrDefault("stateUpdatedAt"), 0L);
        var stateLsn = ParseLongOrDefault(map.GetValueOrDefault("stateLsn"), 0L);
        var lat = ParseNullableDouble(map.GetValueOrDefault("lat"));
        var lng = ParseNullableDouble(map.GetValueOrDefault("lng"));
        var posRecordedAt = ParseLongOrDefault(map.GetValueOrDefault("posRecordedAt"), 0L);
        var posLsn = ParseLongOrDefault(map.GetValueOrDefault("posLsn"), 0L);
        var isDeleted = string.Equals(map.GetValueOrDefault("deleted"), "1", StringComparison.Ordinal);
        var deletedLsn = ParseLongOrDefault(map.GetValueOrDefault("deletedLsn"), 0L);

        return new VehicleReadModel(
            VehicleId: id,
            Plate: plate,
            State: state,
            StateUpdatedAtUnixMs: stateUpdatedAt,
            StateLsn: stateLsn,
            Latitude: lat,
            Longitude: lng,
            PosRecordedAtUnixMs: posRecordedAt,
            PosLsn: posLsn,
            IsDeleted: isDeleted,
            DeletedLsn: deletedLsn);
    }

    private static long ParseLongOrDefault(string? value, long fallback) =>
        !string.IsNullOrWhiteSpace(value) && long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;

    private static double? ParseNullableDouble(string? value) =>
        !string.IsNullOrWhiteSpace(value) && double.TryParse(value, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;

    private static string LoadEmbeddedLuaScript(string scriptFileName)
    {
        var assembly = typeof(ValkeyLuaProjector).Assembly;
        var resourceName = assembly
            .GetManifestResourceNames()
            .FirstOrDefault(n => n.EndsWith(scriptFileName, StringComparison.OrdinalIgnoreCase))
            ?? throw new InvalidOperationException($"Embedded Lua script '{scriptFileName}' not found in assembly.");

        using var stream = assembly.GetManifestResourceStream(resourceName)!;
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
