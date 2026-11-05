using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;
using VehicleCache.Core.Abstractions;
using VehicleCache.Core.Domain;
using VehicleCache.Core.Parsing;
using VehicleCache.Infrastructure.Messaging;
using VehicleCache.Infrastructure.Projection;
using Xunit;

namespace VehicleCache.Tests;

/// <summary>
/// Automated test suite verifying all 8 mandatory behaviors specified in
/// <c>docs/03-test-behaviors.md</c> (B1 through B8) plus Debezium CDC envelope parsing.
/// </summary>
public sealed class VehicleProjectionTests
{
    private static readonly Guid V1 = Guid.Parse("11111111-1111-1111-1111-111111111111");
    private static readonly Guid V2 = Guid.Parse("22222222-2222-2222-2222-222222222222");
    private static readonly Guid V3 = Guid.Parse("33333333-3333-3333-3333-333333333333");

    [Fact]
    public async Task B1_HappyPath_VehicleLifecycle_TransitionsMonotonically()
    {
        var projector = new InMemoryAtomicProjector();
        var states = new[]
        {
            (State: VehicleStates.Registered,     Op: "c", Ts: 1000L, Lsn: 100L),
            (State: VehicleStates.Active,         Op: "u", Ts: 2000L, Lsn: 110L),
            (State: VehicleStates.Idle,           Op: "u", Ts: 3000L, Lsn: 120L),
            (State: VehicleStates.Maintenance,    Op: "u", Ts: 4000L, Lsn: 130L),
            (State: VehicleStates.Decommissioned, Op: "u", Ts: 5000L, Lsn: 140L)
        };

        foreach (var step in states)
        {
            var outcome = await projector.ProjectVehicleAsync(new VehicleCdcEvent(
                Operation: step.Op,
                VehicleId: V1,
                Plate: "NST-1001",
                State: step.State,
                UpdatedAtUnixMs: step.Ts,
                Lsn: step.Lsn,
                SourceTsMs: step.Ts));

            Assert.Equal(ProjectionOutcome.Applied, outcome);

            var current = await projector.GetVehicleAsync(V1);
            Assert.NotNull(current);
            Assert.Equal(step.State, current!.State);
            Assert.Equal(step.Ts, current.StateUpdatedAtUnixMs);
            Assert.Equal(step.Lsn, current.StateLsn);
            Assert.False(current.IsDeleted);
        }
    }

    [Fact]
    public async Task B2_HappyPath_PositionUpdates_AdvanceCoordinatesInSingleReadModel()
    {
        var projector = new InMemoryAtomicProjector();

        await projector.ProjectVehicleAsync(new VehicleCdcEvent(
            Operation: "c", VehicleId: V1, Plate: "NST-1001", State: VehicleStates.Active,
            UpdatedAtUnixMs: 1000L, Lsn: 100L, SourceTsMs: 1000L));

        var positions = new[]
        {
            (Lat: -23.5505, Lng: -46.6333, Ts: 1100L, Lsn: 200L),
            (Lat: -23.5610, Lng: -46.6400, Ts: 1200L, Lsn: 210L),
            (Lat: -23.5725, Lng: -46.6512, Ts: 1300L, Lsn: 220L)
        };

        foreach (var pos in positions)
        {
            var outcome = await projector.ProjectPositionAsync(new PositionCdcEvent(
                Operation: "c", VehicleId: V1, Latitude: pos.Lat, Longitude: pos.Lng,
                RecordedAtUnixMs: pos.Ts, Lsn: pos.Lsn, SourceTsMs: pos.Ts));

            Assert.Equal(ProjectionOutcome.Applied, outcome);
        }

        var readModel = await projector.GetVehicleAsync(V1);
        Assert.NotNull(readModel);
        Assert.Equal(VehicleStates.Active, readModel!.State);
        Assert.Equal(-23.5725, readModel.Latitude);
        Assert.Equal(-46.6512, readModel.Longitude);
        Assert.Equal(1300L, readModel.PosRecordedAtUnixMs);
        Assert.Equal(220L, readModel.PosLsn);
    }

    [Fact]
    public async Task B3_OutOfOrder_StalePosition_IsRejected()
    {
        var projector = new InMemoryAtomicProjector();

        // Newer position arrives first
        await projector.ProjectPositionAsync(new PositionCdcEvent(
            Operation: "c", VehicleId: V1, Latitude: -23.5725, Longitude: -46.6512,
            RecordedAtUnixMs: 2000L, Lsn: 220L, SourceTsMs: 2000L));

        // Older position arrives late (even with higher LSN 230 from late DB insert)
        var staleOutcome = await projector.ProjectPositionAsync(new PositionCdcEvent(
            Operation: "c", VehicleId: V1, Latitude: -10.0000, Longitude: -10.0000,
            RecordedAtUnixMs: 1500L, Lsn: 230L, SourceTsMs: 2100L));

        Assert.Equal(ProjectionOutcome.SkippedStale, staleOutcome);

        var readModel = await projector.GetVehicleAsync(V1);
        Assert.NotNull(readModel);
        Assert.Equal(-23.5725, readModel!.Latitude);
        Assert.Equal(-46.6512, readModel.Longitude);
        Assert.Equal(2000L, readModel.PosRecordedAtUnixMs);
        Assert.Equal(220L, readModel.PosLsn);
    }

    [Fact]
    public async Task B4_OutOfOrder_StaleStateTransition_IsRejected()
    {
        var projector = new InMemoryAtomicProjector();

        await projector.ProjectVehicleAsync(new VehicleCdcEvent(
            Operation: "u", VehicleId: V1, Plate: "NST-1001", State: VehicleStates.Maintenance,
            UpdatedAtUnixMs: 5000L, Lsn: 130L, SourceTsMs: 5000L));

        // Stale state transition arrives out-of-order
        var staleOutcome = await projector.ProjectVehicleAsync(new VehicleCdcEvent(
            Operation: "u", VehicleId: V1, Plate: "NST-1001", State: VehicleStates.Active,
            UpdatedAtUnixMs: 3000L, Lsn: 140L, SourceTsMs: 5100L));

        Assert.Equal(ProjectionOutcome.SkippedStale, staleOutcome);

        var readModel = await projector.GetVehicleAsync(V1);
        Assert.NotNull(readModel);
        Assert.Equal(VehicleStates.Maintenance, readModel!.State);
        Assert.Equal(5000L, readModel.StateUpdatedAtUnixMs);
        Assert.Equal(130L, readModel.StateLsn);
    }

    [Fact]
    public async Task B5_DuplicateDelivery_And_InitialSnapshot_AreIdempotentNoOps()
    {
        var projector = new InMemoryAtomicProjector();

        var vehicleEvt = new VehicleCdcEvent(
            Operation: "r", VehicleId: V1, Plate: "NST-1001", State: VehicleStates.Active,
            UpdatedAtUnixMs: 2000L, Lsn: 110L, SourceTsMs: 2000L);

        var posEvt = new PositionCdcEvent(
            Operation: "r", VehicleId: V1, Latitude: -23.5505, Longitude: -46.6333,
            RecordedAtUnixMs: 2500L, Lsn: 210L, SourceTsMs: 2500L);

        Assert.Equal(ProjectionOutcome.Applied, await projector.ProjectVehicleAsync(vehicleEvt));
        Assert.Equal(ProjectionOutcome.SkippedDuplicate, await projector.ProjectVehicleAsync(vehicleEvt with { Operation = "c" }));

        Assert.Equal(ProjectionOutcome.Applied, await projector.ProjectPositionAsync(posEvt));
        Assert.Equal(ProjectionOutcome.SkippedDuplicate, await projector.ProjectPositionAsync(posEvt with { Operation = "c" }));
    }

    [Fact]
    public async Task B6_CrossStreamRace_PositionBeforeVehicle_MergesCleanly()
    {
        var projector = new InMemoryAtomicProjector();

        // 1. Position arrives BEFORE vehicle creation CDC event
        var posOutcome = await projector.ProjectPositionAsync(new PositionCdcEvent(
            Operation: "c", VehicleId: V2, Latitude: -25.4284, Longitude: -49.2733,
            RecordedAtUnixMs: 3000L, Lsn: 310L, SourceTsMs: 3000L));

        Assert.Equal(ProjectionOutcome.Applied, posOutcome);

        var partial = await projector.GetVehicleAsync(V2);
        Assert.NotNull(partial);
        Assert.Equal(VehicleStates.Unknown, partial!.State);
        Assert.Equal(-25.4284, partial.Latitude);
        Assert.Equal(-49.2733, partial.Longitude);

        // 2. Vehicle creation arrives later (with earlier LSN 300)
        var vehOutcome = await projector.ProjectVehicleAsync(new VehicleCdcEvent(
            Operation: "c", VehicleId: V2, Plate: "NST-2002", State: VehicleStates.Registered,
            UpdatedAtUnixMs: 2900L, Lsn: 300L, SourceTsMs: 2900L));

        Assert.Equal(ProjectionOutcome.Applied, vehOutcome);

        var merged = await projector.GetVehicleAsync(V2);
        Assert.NotNull(merged);
        Assert.Equal(VehicleStates.Registered, merged!.State);
        Assert.Equal("NST-2002", merged.Plate);
        Assert.Equal(300L, merged.StateLsn);
        // Position coordinates must remain intact!
        Assert.Equal(-25.4284, merged.Latitude);
        Assert.Equal(-49.2733, merged.Longitude);
        Assert.Equal(3000L, merged.PosRecordedAtUnixMs);
        Assert.Equal(310L, merged.PosLsn);
    }

    [Fact]
    public async Task B7_DeletedVehicle_SoftTombstones_And_RejectsLatePreDeleteEvents()
    {
        var projector = new InMemoryAtomicProjector();
        var dispatcher = new CdcEventDispatcher(projector, NullLogger<CdcEventDispatcher>.Instance);

        // 1. Vehicle V3 is registered and gets a position
        await projector.ProjectVehicleAsync(new VehicleCdcEvent(
            Operation: "c", VehicleId: V3, Plate: "NST-3003", State: VehicleStates.Idle,
            UpdatedAtUnixMs: 1000L, Lsn: 400L, SourceTsMs: 1000L));
        await projector.ProjectPositionAsync(new PositionCdcEvent(
            Operation: "c", VehicleId: V3, Latitude: -19.9167, Longitude: -43.9345,
            RecordedAtUnixMs: 1100L, Lsn: 410L, SourceTsMs: 1100L));

        // 2. Vehicle V3 is DELETED at LSN = 450
        var delOutcome = await projector.ProjectVehicleAsync(new VehicleCdcEvent(
            Operation: "d", VehicleId: V3, Plate: "NST-3003", State: VehicleStates.Idle,
            UpdatedAtUnixMs: 1000L, Lsn: 450L, SourceTsMs: 1500L));

        Assert.Equal(ProjectionOutcome.Applied, delOutcome);

        // 3. Subsequent Kafka log-compaction null tombstone is safely ignored
        var kafkaTombstoneOutcome = await dispatcher.HandleVehiclePayloadAsync(null);
        Assert.Equal(ProjectionOutcome.IgnoredKafkaTombstone, kafkaTombstoneOutcome);

        // 4. Late pre-delete position (LSN 420 < 450) and state update (LSN 430 < 450) arrive out-of-order
        var latePosOutcome = await projector.ProjectPositionAsync(new PositionCdcEvent(
            Operation: "c", VehicleId: V3, Latitude: -19.9999, Longitude: -43.9999,
            RecordedAtUnixMs: 1200L, Lsn: 420L, SourceTsMs: 1200L));

        var lateVehOutcome = await projector.ProjectVehicleAsync(new VehicleCdcEvent(
            Operation: "u", VehicleId: V3, Plate: "NST-3003", State: VehicleStates.Active,
            UpdatedAtUnixMs: 1300L, Lsn: 430L, SourceTsMs: 1300L));

        Assert.Equal(ProjectionOutcome.SkippedTombstoned, latePosOutcome);
        Assert.Equal(ProjectionOutcome.SkippedTombstoned, lateVehOutcome);

        var deletedModel = await projector.GetVehicleAsync(V3);
        Assert.NotNull(deletedModel);
        Assert.True(deletedModel!.IsDeleted);
        Assert.Equal(VehicleStates.Deleted, deletedModel.State);
        Assert.Null(deletedModel.Latitude);
        Assert.Null(deletedModel.Longitude);
        Assert.Equal(450L, deletedModel.DeletedLsn);
    }

    [Fact]
    public async Task B8_FullReplayFromOffsetZero_RebuildsIdenticalValkeyState_UnderAnyCrossStreamOrder()
    {
        var projector = new InMemoryAtomicProjector();
        var dispatcher = new CdcEventDispatcher(projector, NullLogger<CdcEventDispatcher>.Instance);

        var (vehiclePayloads, positionPayloads) = BuildDeterministicDebeziumLog();

        // Initial streaming pass (interleaved)
        for (var i = 0; i < Math.Max(vehiclePayloads.Count, positionPayloads.Count); i++)
        {
            if (i < vehiclePayloads.Count)
            {
                await dispatcher.HandleVehiclePayloadAsync(vehiclePayloads[i]);
            }
            if (i < positionPayloads.Count)
            {
                await dispatcher.HandlePositionPayloadAsync(positionPayloads[i]);
            }
        }

        var snapshotA = await projector.SnapshotAllAsync();
        Assert.Equal(3, snapshotA.Count);

        // Flush cache and replay from offset 0 (all positions first, then all vehicles — worst-case cross-stream skew!)
        await projector.FlushAllAsync();
        Assert.Empty(await projector.SnapshotAllAsync());

        foreach (var posJson in positionPayloads)
        {
            await dispatcher.HandlePositionPayloadAsync(posJson);
        }
        foreach (var vehJson in vehiclePayloads)
        {
            await dispatcher.HandleVehiclePayloadAsync(vehJson);
        }

        var snapshotB = await projector.SnapshotAllAsync();

        // Snapshot B must be 100% identical to Snapshot A
        Assert.Equal(snapshotA.Count, snapshotB.Count);
        foreach (var (id, modelA) in snapshotA)
        {
            Assert.True(snapshotB.TryGetValue(id, out var modelB));
            Assert.Equal(modelA, modelB);
        }
    }

    private static (IReadOnlyList<string?> VehiclePayloads, IReadOnlyList<string?> PositionPayloads) BuildDeterministicDebeziumLog()
    {
        var vehiclePayloads = new List<string?>
        {
            // V1 lifecycle + stale update
            $$$"""{"payload":{"before":null,"after":{"id":"{{{V1}}}","plate":"NST-1001","state":"REGISTERED","updated_at":"2026-11-05T12:00:00Z"},"source":{"lsn":100,"ts_ms":1762344000000},"op":"r"}}""",
            $$$"""{"payload":{"before":{"id":"{{{V1}}}"},"after":{"id":"{{{V1}}}","plate":"NST-1001","state":"ACTIVE","updated_at":"2026-11-05T12:05:00Z"},"source":{"lsn":110,"ts_ms":1762344300000},"op":"u"}}""",
            $$$"""{"payload":{"before":{"id":"{{{V1}}}"},"after":{"id":"{{{V1}}}","plate":"NST-1001","state":"DECOMMISSIONED","updated_at":"2026-11-05T13:00:00Z"},"source":{"lsn":140,"ts_ms":1762347600000},"op":"u"}}""",
            $$$"""{"payload":{"before":{"id":"{{{V1}}}"},"after":{"id":"{{{V1}}}","plate":"NST-1001","state":"IDLE","updated_at":"2026-11-05T12:10:00Z"},"source":{"lsn":150,"ts_ms":1762347660000},"op":"u"}}""",
            // V2 created after position
            $$$"""{"payload":{"before":null,"after":{"id":"{{{V2}}}","plate":"NST-2002","state":"ACTIVE","updated_at":"2026-11-05T12:10:00Z"},"source":{"lsn":300,"ts_ms":1762344600000},"op":"c"}}""",
            // V3 created then deleted + Kafka null tombstone
            $$$"""{"payload":{"before":null,"after":{"id":"{{{V3}}}","plate":"NST-3003","state":"IDLE","updated_at":"2026-11-05T12:02:00Z"},"source":{"lsn":400,"ts_ms":1762344120000},"op":"c"}}""",
            $$$"""{"payload":{"before":{"id":"{{{V3}}}","plate":"NST-3003","state":"IDLE","updated_at":"2026-11-05T12:02:00Z"},"after":null,"source":{"lsn":450,"ts_ms":1762344900000},"op":"d"}}""",
            null
        };

        var positionPayloads = new List<string?>
        {
            // V1 positions + duplicate + stale
            $$$"""{"payload":{"before":null,"after":{"vehicle_id":"{{{V1}}}","lat":-23.5505,"lng":-46.6333,"recorded_at":"2026-11-05T12:06:00Z"},"source":{"lsn":200,"ts_ms":1762344360000},"op":"c"}}""",
            $$$"""{"payload":{"before":null,"after":{"vehicle_id":"{{{V1}}}","lat":-23.5725,"lng":-46.6512,"recorded_at":"2026-11-05T12:08:00Z"},"source":{"lsn":220,"ts_ms":1762344480000},"op":"c"}}""",
            $$$"""{"payload":{"before":null,"after":{"vehicle_id":"{{{V1}}}","lat":-23.5725,"lng":-46.6512,"recorded_at":"2026-11-05T12:08:00Z"},"source":{"lsn":220,"ts_ms":1762344480000},"op":"c"}}""",
            $$$"""{"payload":{"before":null,"after":{"vehicle_id":"{{{V1}}}","lat":-23.0000,"lng":-46.0000,"recorded_at":"2026-11-05T12:06:30Z"},"source":{"lsn":230,"ts_ms":1762344500000},"op":"c"}}""",
            // V2 position arrives before vehicle
            $$$"""{"payload":{"before":null,"after":{"vehicle_id":"{{{V2}}}","lat":-25.4284,"lng":-49.2733,"recorded_at":"2026-11-05T12:12:00Z"},"source":{"lsn":310,"ts_ms":1762344720000},"op":"c"}}""",
            // V3 pre-delete position
            $$$"""{"payload":{"before":null,"after":{"vehicle_id":"{{{V3}}}","lat":-19.9167,"lng":-43.9345,"recorded_at":"2026-11-05T12:03:00Z"},"source":{"lsn":410,"ts_ms":1762344180000},"op":"c"}}"""
        };

        return (vehiclePayloads, positionPayloads);
    }
}
