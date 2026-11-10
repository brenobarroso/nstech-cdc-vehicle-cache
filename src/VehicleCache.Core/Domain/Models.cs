namespace VehicleCache.Core.Domain;

/// <summary>
/// Canonical lifecycle states for a vehicle in the read model.
/// </summary>
public static class VehicleStates
{
    public const string Registered = "REGISTERED";
    public const string Active = "ACTIVE";
    public const string Idle = "IDLE";
    public const string Maintenance = "MAINTENANCE";
    public const string Decommissioned = "DECOMMISSIONED";

    /// <summary>
    /// Assigned when a position CDC event arrives before the vehicle registration CDC event (cross-stream race).
    /// </summary>
    public const string Unknown = "UNKNOWN";

    /// <summary>
    /// Assigned when a vehicle row is deleted in PostgreSQL (soft-tombstone marker).
    /// </summary>
    public const string Deleted = "DELETED";
}

/// <summary>
/// Result of projecting a CDC event into the Valkey read model.
/// </summary>
public enum ProjectionOutcome
{
    Applied = 1,
    SkippedDuplicate = 0,
    SkippedStale = -1,
    SkippedTombstoned = -2,
    IgnoredKafkaTombstone = -3
}

/// <summary>
/// Parsed Debezium CDC event from the <c>cdc.public.vehicles</c> topic.
/// </summary>
public sealed record VehicleCdcEvent(
    string Operation,
    Guid VehicleId,
    string Plate,
    string State,
    long UpdatedAtUnixMs,
    long Lsn,
    long SourceTsMs
)
{
    public bool IsDelete => string.Equals(Operation, "d", StringComparison.OrdinalIgnoreCase);
}

/// <summary>
/// Parsed Debezium CDC event from the <c>cdc.public.positions</c> topic.
/// </summary>
public sealed record PositionCdcEvent(
    string Operation,
    Guid VehicleId,
    double Latitude,
    double Longitude,
    long RecordedAtUnixMs,
    long Lsn,
    long SourceTsMs
);

/// <summary>
/// Unified single-key Valkey read model (`vehicle:{id}`) answering:
/// "Where is this vehicle right now, and what state is it in?"
/// </summary>
public sealed record VehicleReadModel(
    Guid VehicleId,
    string Plate,
    string State,
    long StateUpdatedAtUnixMs,
    long StateLsn,
    double? Latitude,
    double? Longitude,
    long PosRecordedAtUnixMs,
    long PosLsn,
    bool IsDeleted,
    long DeletedLsn
)
{
    public static string GetKey(Guid vehicleId) => $"vehicle:{vehicleId:D}";
}
