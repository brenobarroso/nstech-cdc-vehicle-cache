using VehicleCache.Core.Abstractions;
using VehicleCache.Core.Domain;

namespace VehicleCache.Infrastructure.Projection;

/// <summary>
/// Thread-safe in-memory reference implementation of the exact Valkey Lua projection scripts
/// (<c>UpsertOrDeleteVehicle.lua</c> and <c>UpsertPosition.lua</c>) for fast deterministic unit testing.
/// </summary>
public sealed class InMemoryAtomicProjector : IVehicleReadModelProjector
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly Dictionary<Guid, VehicleReadModel> _store = new();

    public async Task<ProjectionOutcome> ProjectVehicleAsync(VehicleCdcEvent evt, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(evt);
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var exists = _store.TryGetValue(evt.VehicleId, out var current);
            var curDeleted = exists && current!.IsDeleted;
            var curDeletedLsn = exists ? current!.DeletedLsn : 0L;
            var curStateUpdatedAt = exists ? current!.StateUpdatedAtUnixMs : 0L;
            var curStateLsn = exists ? current!.StateLsn : 0L;

            if (evt.IsDelete)
            {
                if (curDeleted && evt.Lsn == curDeletedLsn)
                {
                    return ProjectionOutcome.SkippedDuplicate;
                }
                if (evt.Lsn < curDeletedLsn || evt.Lsn < curStateLsn)
                {
                    return ProjectionOutcome.SkippedStale;
                }

                _store[evt.VehicleId] = new VehicleReadModel(
                    VehicleId: evt.VehicleId,
                    Plate: string.Empty,
                    State: VehicleStates.Deleted,
                    StateUpdatedAtUnixMs: 0L,
                    StateLsn: evt.Lsn,
                    Latitude: null,
                    Longitude: null,
                    PosRecordedAtUnixMs: 0L,
                    PosLsn: 0L,
                    IsDeleted: true,
                    DeletedLsn: evt.Lsn);

                return ProjectionOutcome.Applied;
            }

            if (curDeleted && evt.Lsn <= curDeletedLsn)
            {
                return ProjectionOutcome.SkippedTombstoned;
            }

            if (curStateLsn > 0 && evt.UpdatedAtUnixMs == curStateUpdatedAt && evt.Lsn == curStateLsn)
            {
                return ProjectionOutcome.SkippedDuplicate;
            }

            var isNewer = curStateLsn == 0
                || evt.UpdatedAtUnixMs > curStateUpdatedAt
                || (evt.UpdatedAtUnixMs == curStateUpdatedAt && evt.Lsn > curStateLsn);

            if (!isNewer)
            {
                return ProjectionOutcome.SkippedStale;
            }

            if (!exists)
            {
                _store[evt.VehicleId] = new VehicleReadModel(
                    VehicleId: evt.VehicleId,
                    Plate: evt.Plate,
                    State: evt.State,
                    StateUpdatedAtUnixMs: evt.UpdatedAtUnixMs,
                    StateLsn: evt.Lsn,
                    Latitude: null,
                    Longitude: null,
                    PosRecordedAtUnixMs: 0L,
                    PosLsn: 0L,
                    IsDeleted: false,
                    DeletedLsn: 0L);
            }
            else
            {
                _store[evt.VehicleId] = current! with
                {
                    Plate = evt.Plate,
                    State = evt.State,
                    StateUpdatedAtUnixMs = evt.UpdatedAtUnixMs,
                    StateLsn = evt.Lsn,
                    IsDeleted = false
                };
            }

            return ProjectionOutcome.Applied;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<ProjectionOutcome> ProjectPositionAsync(PositionCdcEvent evt, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(evt);
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var exists = _store.TryGetValue(evt.VehicleId, out var current);
            var curDeleted = exists && current!.IsDeleted;
            var curDeletedLsn = exists ? current!.DeletedLsn : 0L;
            var curPosRecordedAt = exists ? current!.PosRecordedAtUnixMs : 0L;
            var curPosLsn = exists ? current!.PosLsn : 0L;

            if (curDeleted || (curDeletedLsn > 0 && evt.Lsn <= curDeletedLsn))
            {
                return ProjectionOutcome.SkippedTombstoned;
            }

            if (curPosLsn > 0 && evt.RecordedAtUnixMs == curPosRecordedAt && evt.Lsn == curPosLsn)
            {
                return ProjectionOutcome.SkippedDuplicate;
            }

            var isNewer = curPosLsn == 0
                || evt.RecordedAtUnixMs > curPosRecordedAt
                || (evt.RecordedAtUnixMs == curPosRecordedAt && evt.Lsn > curPosLsn);

            if (!isNewer)
            {
                return ProjectionOutcome.SkippedStale;
            }

            if (!exists)
            {
                _store[evt.VehicleId] = new VehicleReadModel(
                    VehicleId: evt.VehicleId,
                    Plate: string.Empty,
                    State: VehicleStates.Unknown,
                    StateUpdatedAtUnixMs: 0L,
                    StateLsn: 0L,
                    Latitude: evt.Latitude,
                    Longitude: evt.Longitude,
                    PosRecordedAtUnixMs: evt.RecordedAtUnixMs,
                    PosLsn: evt.Lsn,
                    IsDeleted: false,
                    DeletedLsn: 0L);
            }
            else
            {
                _store[evt.VehicleId] = current! with
                {
                    Latitude = evt.Latitude,
                    Longitude = evt.Longitude,
                    PosRecordedAtUnixMs = evt.RecordedAtUnixMs,
                    PosLsn = evt.Lsn
                };
            }

            return ProjectionOutcome.Applied;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<VehicleReadModel?> GetVehicleAsync(Guid vehicleId, CancellationToken ct = default)
    {
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return _store.TryGetValue(vehicleId, out var model) ? model : null;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<IReadOnlyDictionary<Guid, VehicleReadModel>> SnapshotAllAsync(CancellationToken ct = default)
    {
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return new Dictionary<Guid, VehicleReadModel>(_store);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task FlushAllAsync(CancellationToken ct = default)
    {
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            _store.Clear();
        }
        finally
        {
            _gate.Release();
        }
    }
}
