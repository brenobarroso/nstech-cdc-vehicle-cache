using Npgsql;

namespace VehicleCache.MockWriters;

/// <summary>
/// Mock Position Writer: writes exclusively to PostgreSQL <c>public.positions</c> (insert-only GPS telemetry).
/// Supports injecting out-of-order <c>recorded_at</c> timestamps, duplicate coordinates, and positions
/// for vehicles not yet present in <c>public.vehicles</c> (cross-stream race).
/// </summary>
public sealed class PostgresPositionWriter
{
    private readonly string _connectionString;

    public PostgresPositionWriter(string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        _connectionString = connectionString;
    }

    public async Task InsertPositionAsync(
        Guid vehicleId,
        double latitude,
        double longitude,
        DateTimeOffset recordedAt,
        CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);

        const string sql = """
            INSERT INTO public.positions (vehicle_id, lat, lng, recorded_at)
            VALUES (@vehicle_id, @lat, @lng, @recorded_at);
            """;

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("vehicle_id", vehicleId);
        cmd.Parameters.AddWithValue("lat", latitude);
        cmd.Parameters.AddWithValue("lng", longitude);
        cmd.Parameters.AddWithValue("recorded_at", recordedAt.UtcDateTime);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}
