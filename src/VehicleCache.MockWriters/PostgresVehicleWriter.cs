using Npgsql;

namespace VehicleCache.MockWriters;

/// <summary>
/// Mock Vehicle Writer: writes exclusively to PostgreSQL <c>public.vehicles</c>
/// (INSERT, UPDATE state transitions, and DELETE), driving Debezium CDC via WAL.
/// </summary>
public sealed class PostgresVehicleWriter
{
    private readonly string _connectionString;

    public PostgresVehicleWriter(string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        _connectionString = connectionString;
    }

    public async Task EnsureSchemaAsync(CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);

        const string sql = """
            CREATE TABLE IF NOT EXISTS public.vehicles (
                id              UUID PRIMARY KEY,
                plate           VARCHAR(16) NOT NULL,
                state           VARCHAR(32) NOT NULL,
                updated_at      TIMESTAMPTZ NOT NULL
            );

            ALTER TABLE public.vehicles REPLICA IDENTITY FULL;

            CREATE TABLE IF NOT EXISTS public.positions (
                id              BIGSERIAL PRIMARY KEY,
                vehicle_id      UUID NOT NULL,
                lat             DOUBLE PRECISION NOT NULL,
                lng             DOUBLE PRECISION NOT NULL,
                recorded_at     TIMESTAMPTZ NOT NULL
            );

            CREATE INDEX IF NOT EXISTS ix_positions_vehicle_recorded
                ON public.positions (vehicle_id, recorded_at DESC);
            """;

        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async Task TruncateTablesAsync(CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);

        await using var cmd = new NpgsqlCommand("TRUNCATE TABLE public.positions, public.vehicles RESTART IDENTITY;", conn);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async Task InsertVehicleAsync(
        Guid vehicleId,
        string plate,
        string state,
        DateTimeOffset updatedAt,
        CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);

        const string sql = """
            INSERT INTO public.vehicles (id, plate, state, updated_at)
            VALUES (@id, @plate, @state, @updated_at)
            ON CONFLICT (id) DO UPDATE
            SET plate = EXCLUDED.plate,
                state = EXCLUDED.state,
                updated_at = EXCLUDED.updated_at;
            """;

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", vehicleId);
        cmd.Parameters.AddWithValue("plate", plate);
        cmd.Parameters.AddWithValue("state", state);
        cmd.Parameters.AddWithValue("updated_at", updatedAt.UtcDateTime);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Updates a vehicle's state and <c>updated_at</c>. Supports injecting out-of-order (stale)
    /// <paramref name="updatedAt"/> timestamps to verify monotonic LSN/timestamp filtering in Valkey.
    /// </summary>
    public async Task UpdateVehicleStateAsync(
        Guid vehicleId,
        string state,
        DateTimeOffset updatedAt,
        CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);

        const string sql = """
            UPDATE public.vehicles
            SET state = @state,
                updated_at = @updated_at
            WHERE id = @id;
            """;

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", vehicleId);
        cmd.Parameters.AddWithValue("state", state);
        cmd.Parameters.AddWithValue("updated_at", updatedAt.UtcDateTime);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async Task DeleteVehicleAsync(Guid vehicleId, CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);

        const string sql = "DELETE FROM public.vehicles WHERE id = @id;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("id", vehicleId);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}
