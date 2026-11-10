using VehicleCache.Core.Domain;

namespace VehicleCache.Core.Abstractions;

/// <summary>
/// Port for projecting vehicle and position CDC events into the read-model cache
/// and querying snapshots for verification.
/// </summary>
public interface IVehicleReadModelProjector
{
    Task<ProjectionOutcome> ProjectVehicleAsync(VehicleCdcEvent evt, CancellationToken ct = default);
    Task<ProjectionOutcome> ProjectPositionAsync(PositionCdcEvent evt, CancellationToken ct = default);
    Task<VehicleReadModel?> GetVehicleAsync(Guid vehicleId, CancellationToken ct = default);
    Task<IReadOnlyDictionary<Guid, VehicleReadModel>> SnapshotAllAsync(CancellationToken ct = default);
    Task FlushAllAsync(CancellationToken ct = default);
}
