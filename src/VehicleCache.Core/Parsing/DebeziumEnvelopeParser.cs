using System.Globalization;
using System.Text.Json;
using VehicleCache.Core.Domain;

namespace VehicleCache.Core.Parsing;

/// <summary>
/// Parses Debezium PostgreSQL CDC JSON envelopes for <c>vehicles</c> and <c>positions</c>.
/// Supports both wrapped (<c>{ "schema": ..., "payload": { ... } }</c>) and unwrapped
/// (<c>{ "before": ..., "after": ..., "source": ..., "op": ... }</c>) formats, as well as
/// Kafka compaction tombstones (<c>null</c> or empty payload).
/// </summary>
public static class DebeziumEnvelopeParser
{
    public static VehicleCdcEvent? ParseVehicleEvent(string? rawJson)
    {
        if (string.IsNullOrWhiteSpace(rawJson))
        {
            return null; // Kafka log-compaction tombstone
        }

        using var doc = JsonDocument.Parse(rawJson);
        var root = UnwrapPayload(doc.RootElement);
        if (root.ValueKind == JsonValueKind.Null || root.ValueKind == JsonValueKind.Undefined)
        {
            return null;
        }

        if (!root.TryGetProperty("op", out var opProp) || opProp.ValueKind != JsonValueKind.String)
        {
            return null;
        }

        var op = opProp.GetString()!;
        var (lsn, sourceTsMs) = ExtractSourceMetadata(root);

        if (string.Equals(op, "d", StringComparison.OrdinalIgnoreCase))
        {
            if (!root.TryGetProperty("before", out var before) || before.ValueKind != JsonValueKind.Object)
            {
                throw new InvalidOperationException("Debezium DELETE ('d') event on vehicles is missing 'before' object. Ensure REPLICA IDENTITY FULL is enabled.");
            }

            var deletedId = ParseGuid(before, "id");
            var deletedPlate = GetStringOrDefault(before, "plate", string.Empty);
            var deletedState = GetStringOrDefault(before, "state", VehicleStates.Deleted);
            var deletedUpdatedAt = ParseTimestampMs(before, "updated_at", sourceTsMs);

            return new VehicleCdcEvent(
                Operation: "d",
                VehicleId: deletedId,
                Plate: deletedPlate,
                State: deletedState,
                UpdatedAtUnixMs: deletedUpdatedAt,
                Lsn: lsn,
                SourceTsMs: sourceTsMs);
        }

        if (!root.TryGetProperty("after", out var after) || after.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        var id = ParseGuid(after, "id");
        var plate = GetStringOrDefault(after, "plate", string.Empty);
        var state = GetStringOrDefault(after, "state", VehicleStates.Registered);
        var updatedAtMs = ParseTimestampMs(after, "updated_at", sourceTsMs);

        return new VehicleCdcEvent(
            Operation: op,
            VehicleId: id,
            Plate: plate,
            State: state,
            UpdatedAtUnixMs: updatedAtMs,
            Lsn: lsn,
            SourceTsMs: sourceTsMs);
    }

    public static PositionCdcEvent? ParsePositionEvent(string? rawJson)
    {
        if (string.IsNullOrWhiteSpace(rawJson))
        {
            return null; // Kafka log-compaction tombstone
        }

        using var doc = JsonDocument.Parse(rawJson);
        var root = UnwrapPayload(doc.RootElement);
        if (root.ValueKind == JsonValueKind.Null || root.ValueKind == JsonValueKind.Undefined)
        {
            return null;
        }

        if (!root.TryGetProperty("op", out var opProp) || opProp.ValueKind != JsonValueKind.String)
        {
            return null;
        }

        var op = opProp.GetString()!;
        if (string.Equals(op, "d", StringComparison.OrdinalIgnoreCase))
        {
            // Positions stream is insert-only per domain rules; ignore deletes on positions table.
            return null;
        }

        if (!root.TryGetProperty("after", out var after) || after.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        var (lsn, sourceTsMs) = ExtractSourceMetadata(root);
        var vehicleId = ParseGuid(after, "vehicle_id");
        var lat = ParseDouble(after, "lat");
        var lng = ParseDouble(after, "lng");
        var recordedAtMs = ParseTimestampMs(after, "recorded_at", sourceTsMs);

        return new PositionCdcEvent(
            Operation: op,
            VehicleId: vehicleId,
            Latitude: lat,
            Longitude: lng,
            RecordedAtUnixMs: recordedAtMs,
            Lsn: lsn,
            SourceTsMs: sourceTsMs);
    }

    private static JsonElement UnwrapPayload(JsonElement root)
    {
        if (root.ValueKind == JsonValueKind.Object &&
            root.TryGetProperty("payload", out var payload))
        {
            return payload;
        }
        return root;
    }

    private static (long Lsn, long SourceTsMs) ExtractSourceMetadata(JsonElement root)
    {
        long lsn = 0;
        long tsMs = 0;

        if (root.TryGetProperty("source", out var source) && source.ValueKind == JsonValueKind.Object)
        {
            if (source.TryGetProperty("lsn", out var lsnProp))
            {
                lsn = lsnProp.ValueKind switch
                {
                    JsonValueKind.Number => lsnProp.GetInt64(),
                    JsonValueKind.String when long.TryParse(lsnProp.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) => parsed,
                    _ => 0
                };
            }

            if (source.TryGetProperty("ts_ms", out var tsProp) && tsProp.ValueKind == JsonValueKind.Number)
            {
                tsMs = tsProp.GetInt64();
            }
        }

        if (tsMs == 0 && root.TryGetProperty("ts_ms", out var rootTs) && rootTs.ValueKind == JsonValueKind.Number)
        {
            tsMs = rootTs.GetInt64();
        }

        return (lsn, tsMs);
    }

    private static Guid ParseGuid(JsonElement obj, string propertyName)
    {
        if (!obj.TryGetProperty(propertyName, out var prop) || prop.ValueKind != JsonValueKind.String)
        {
            throw new InvalidOperationException($"Missing required GUID property '{propertyName}'.");
        }
        return Guid.Parse(prop.GetString()!);
    }

    private static string GetStringOrDefault(JsonElement obj, string propertyName, string fallback)
    {
        if (obj.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String)
        {
            return prop.GetString() ?? fallback;
        }
        return fallback;
    }

    private static double ParseDouble(JsonElement obj, string propertyName)
    {
        if (!obj.TryGetProperty(propertyName, out var prop))
        {
            throw new InvalidOperationException($"Missing required numeric property '{propertyName}'.");
        }

        return prop.ValueKind switch
        {
            JsonValueKind.Number => prop.GetDouble(),
            JsonValueKind.String => double.Parse(prop.GetString()!, CultureInfo.InvariantCulture),
            _ => throw new InvalidOperationException($"Invalid numeric format for '{propertyName}'.")
        };
    }

    /// <summary>
    /// Handles Debezium timestamp representations:
    /// - ISO-8601 strings ("2026-11-05T12:00:00Z")
    /// - Microseconds since epoch (int64 >= 1_000_000_000_000_000, Debezium MicroTimestamp)
    /// - Milliseconds since epoch (int64)
    /// </summary>
    private static long ParseTimestampMs(JsonElement obj, string propertyName, long fallbackMs)
    {
        if (!obj.TryGetProperty(propertyName, out var prop))
        {
            return fallbackMs;
        }

        if (prop.ValueKind == JsonValueKind.String)
        {
            var str = prop.GetString();
            if (string.IsNullOrWhiteSpace(str))
            {
                return fallbackMs;
            }
            if (DateTimeOffset.TryParse(str, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var dto))
            {
                return dto.ToUnixTimeMilliseconds();
            }
            if (long.TryParse(str, NumberStyles.Integer, CultureInfo.InvariantCulture, out var numericStr))
            {
                return NormalizeEpochToMs(numericStr);
            }
        }
        else if (prop.ValueKind == JsonValueKind.Number)
        {
            return NormalizeEpochToMs(prop.GetInt64());
        }

        return fallbackMs;
    }

    private static long NormalizeEpochToMs(long rawValue)
    {
        // Debezium io.debezium.time.MicroTimestamp emits microseconds since Unix epoch (~1.76e15 in 2026)
        if (rawValue >= 100_000_000_000_000L)
        {
            return rawValue / 1_000L;
        }
        return rawValue;
    }
}
