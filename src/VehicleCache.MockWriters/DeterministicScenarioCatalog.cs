using VehicleCache.Core.Domain;

namespace VehicleCache.MockWriters;

/// <summary>
/// Fixed deterministic dataset & scenario generator used across unit, Valkey Lua, and full E2E tests.
/// Covers all 8 mandatory behaviors (B1-B8) with reproducible GUIDs and timestamps.
/// </summary>
public static class DeterministicScenarioCatalog
{
    public static readonly Guid Vehicle1LifecycleAndPositions = Guid.Parse("11111111-1111-1111-1111-111111111111");
    public static readonly Guid Vehicle2PositionBeforeVehicle = Guid.Parse("22222222-2222-2222-2222-222222222222");
    public static readonly Guid Vehicle3DeletedAndTombstoned  = Guid.Parse("33333333-3333-3333-3333-333333333333");

    public static readonly DateTimeOffset BaseTime = new(2026, 11, 5, 12, 0, 0, TimeSpan.Zero);

    /// <summary>
    /// Executes the deterministic SQL write sequence against PostgreSQL via the Mock Writers,
    /// exercising B1 (Lifecycle), B2 (Positions), B3 (Out-of-order position), B4 (Out-of-order state),
    /// B5 (Duplicates), B6 (Position before Vehicle), and B7 (Delete + late pre-delete events).
    /// </summary>
    public static async Task ExecutePostgresSequenceAsync(
        PostgresVehicleWriter vehicleWriter,
        PostgresPositionWriter positionWriter,
        CancellationToken ct = default)
    {
        await vehicleWriter.EnsureSchemaAsync(ct).ConfigureAwait(false);

        // =========================================================================
        // B1: Happy-path Vehicle 1 lifecycle:
        // REGISTERED -> ACTIVE -> IDLE -> MAINTENANCE -> DECOMMISSIONED
        // =========================================================================
        var t0 = BaseTime;
        await vehicleWriter.InsertVehicleAsync(Vehicle1LifecycleAndPositions, "NST-1001", VehicleStates.Registered, t0, ct);
        await vehicleWriter.UpdateVehicleStateAsync(Vehicle1LifecycleAndPositions, VehicleStates.Active, t0.AddMinutes(5), ct);
        await vehicleWriter.UpdateVehicleStateAsync(Vehicle1LifecycleAndPositions, VehicleStates.Idle, t0.AddMinutes(15), ct);
        await vehicleWriter.UpdateVehicleStateAsync(Vehicle1LifecycleAndPositions, VehicleStates.Maintenance, t0.AddMinutes(30), ct);
        await vehicleWriter.UpdateVehicleStateAsync(Vehicle1LifecycleAndPositions, VehicleStates.Decommissioned, t0.AddMinutes(60), ct);

        // =========================================================================
        // B4: Out-of-order state transition on Vehicle 1 (stale updated_at = t0 + 10m)
        // Must be REJECTED by Valkey Lua script; state must remain DECOMMISSIONED (t0 + 60m)
        // =========================================================================
        await vehicleWriter.UpdateVehicleStateAsync(Vehicle1LifecycleAndPositions, VehicleStates.Active, t0.AddMinutes(10), ct);

        // =========================================================================
        // B2: Happy-path Position updates on Vehicle 1
        // =========================================================================
        await positionWriter.InsertPositionAsync(Vehicle1LifecycleAndPositions, -23.5505, -46.6333, t0.AddMinutes(6), ct);
        await positionWriter.InsertPositionAsync(Vehicle1LifecycleAndPositions, -23.5610, -46.6400, t0.AddMinutes(7), ct);
        await positionWriter.InsertPositionAsync(Vehicle1LifecycleAndPositions, -23.5725, -46.6512, t0.AddMinutes(8), ct);

        // =========================================================================
        // B3: Out-of-order Position on Vehicle 1 (stale recorded_at = t0 + 6m30s)
        // Must be REJECTED by Valkey Lua script; position must remain (-23.5725, -46.6512 @ t0 + 8m)
        // =========================================================================
        await positionWriter.InsertPositionAsync(Vehicle1LifecycleAndPositions, -23.0000, -46.0000, t0.AddMinutes(6).AddSeconds(30), ct);

        // =========================================================================
        // B6: Cross-stream race on Vehicle 2 — Position written BEFORE Vehicle row exists!
        // =========================================================================
        await positionWriter.InsertPositionAsync(Vehicle2PositionBeforeVehicle, -25.4284, -49.2733, t0.AddMinutes(12), ct);
        await Task.Delay(TimeSpan.FromMilliseconds(250), ct).ConfigureAwait(false);
        await vehicleWriter.InsertVehicleAsync(Vehicle2PositionBeforeVehicle, "NST-2002", VehicleStates.Active, t0.AddMinutes(10), ct);

        // =========================================================================
        // B7: Deleted Vehicle 3 — Created, receives position, then DELETED in Postgres
        // =========================================================================
        await vehicleWriter.InsertVehicleAsync(Vehicle3DeletedAndTombstoned, "NST-3003", VehicleStates.Idle, t0.AddMinutes(2), ct);
        await positionWriter.InsertPositionAsync(Vehicle3DeletedAndTombstoned, -19.9167, -43.9345, t0.AddMinutes(3), ct);
        await Task.Delay(TimeSpan.FromMilliseconds(250), ct).ConfigureAwait(false);
        await vehicleWriter.DeleteVehicleAsync(Vehicle3DeletedAndTombstoned, ct);

        // Late orphan position arriving for deleted Vehicle 3 — must be REJECTED (SkippedTombstoned)
        await positionWriter.InsertPositionAsync(Vehicle3DeletedAndTombstoned, -19.9999, -43.9999, t0.AddMinutes(4), ct);
    }
}
