using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using StackExchange.Redis;
using VehicleCache.Core.Domain;
using VehicleCache.Infrastructure.Projection;

namespace VehicleCache.MockWriters;

/// <summary>
/// CLI entrypoint for seeding PostgreSQL with the deterministic scenario catalog and
/// executing the end-to-end verification + Offset-0 Replay rebuild check (<c>just e2e</c>).
/// </summary>
public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var postgresConn = Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING")
            ?? "Host=localhost;Port=5432;Database=logistics;Username=postgres;Password=postgres";
        var kafkaBootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS")
            ?? "localhost:9092";
        var valkeyConn = Environment.GetEnvironmentVariable("VALKEY_CONNECTION_STRING")
            ?? "localhost:6379,abortConnect=false,allowAdmin=true";
        var debzConnectUrl = Environment.GetEnvironmentVariable("DEBEZIUM_CONNECT_URL")
            ?? "http://localhost:8083";

        Console.WriteLine("=================================================================");
        Console.WriteLine(" NsTech CDC Vehicle Cache - Mock Writers & E2E Replay Verifier");
        Console.WriteLine("=================================================================");

        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var ct = cts.Token;

        var vehicleWriter = new PostgresVehicleWriter(postgresConn);
        var positionWriter = new PostgresPositionWriter(postgresConn);

        using var mux = await ConnectionMultiplexer.ConnectAsync(valkeyConn);
        var projector = new ValkeyLuaProjector(mux);

        Console.WriteLine("[1/6] Ensuring PostgreSQL schema (public.vehicles, public.positions)...");
        await vehicleWriter.EnsureSchemaAsync(ct);
        await vehicleWriter.TruncateTablesAsync(ct);
        await projector.FlushAllAsync(ct);

        Console.WriteLine("[2/6] Ensuring Debezium PostgreSQL connector is registered and RUNNING...");
        await EnsureDebeziumConnectorAsync(debzConnectUrl, ct);

        Console.WriteLine("[3/6] Executing deterministic SQL scenario sequence (B1..B7) via Mock Writers...");
        await DeterministicScenarioCatalog.ExecutePostgresSequenceAsync(vehicleWriter, positionWriter, ct);

        Console.WriteLine("[4/6] Waiting for .NET 10 Worker to converge Valkey state (Snapshot A)...");
        var snapshotA = await WaitForConvergenceAsync(projector, ct);
        ValidateSnapshotAssertions(snapshotA, "Snapshot A (Initial Streaming Projection)");

        Console.WriteLine("[5/6] Executing B8 Full Replay Test: Flushing Valkey & replaying Kafka from offset 0...");
        await ReplayFromOffsetZeroDirectlyAsync(kafkaBootstrap, projector, ct);

        var snapshotB = await WaitForConvergenceAsync(projector, ct);
        ValidateSnapshotAssertions(snapshotB, "Snapshot B (Post Offset-0 Replay)");

        Console.WriteLine("[6/6] Comparing Snapshot A vs. Snapshot B byte-for-byte...");
        AssertSnapshotsIdentical(snapshotA, snapshotB);

        Console.WriteLine("=================================================================");
        Console.WriteLine(" SUCCESS: All 8 CDC behaviors & Offset-0 Replay verified 100%!");
        Console.WriteLine("=================================================================");
        return 0;
    }

    public static async Task EnsureDebeziumConnectorAsync(string connectBaseUrl, CancellationToken ct)
    {
        using var http = new HttpClient
        {
            BaseAddress = new Uri(connectBaseUrl),
            Timeout = TimeSpan.FromSeconds(10)
        };

        var connectorConfig = """
            {
              "name": "logistics-postgres-cdc",
              "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "plugin.name": "pgoutput",
                "database.hostname": "postgres",
                "database.port": "5432",
                "database.user": "postgres",
                "database.password": "postgres",
                "database.dbname": "logistics",
                "topic.prefix": "cdc",
                "table.include.list": "public.vehicles,public.positions",
                "slot.name": "debezium_logistics_slot",
                "publication.autocreate.mode": "filtered",
                "tombstones.on.delete": "true",
                "decimal.handling.mode": "double",
                "time.precision.mode": "connect",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false"
              }
            }
            """;

        for (var attempt = 1; attempt <= 45; attempt++)
        {
            try
            {
                var statusResp = await http.GetAsync("/connectors/logistics-postgres-cdc/status", ct);
                if (statusResp.IsSuccessStatusCode)
                {
                    var statusJson = await statusResp.Content.ReadAsStringAsync(ct);
                    if (statusJson.Contains("\"RUNNING\"", StringComparison.OrdinalIgnoreCase))
                    {
                        await Task.Delay(TimeSpan.FromSeconds(2), ct);
                        return;
                    }
                }
                else
                {
                    await http.PostAsync(
                        "/connectors",
                        new StringContent(connectorConfig, System.Text.Encoding.UTF8, "application/json"),
                        ct);
                }
            }
            catch
            {
                // Kafka Connect still starting up
            }

            await Task.Delay(TimeSpan.FromSeconds(2), ct);
        }

        throw new TimeoutException($"Debezium Kafka Connect at {connectBaseUrl} did not become ready in time.");
    }

    public static async Task<IReadOnlyDictionary<Guid, VehicleReadModel>> WaitForConvergenceAsync(
        ValkeyLuaProjector projector,
        CancellationToken ct)
    {
        var t0 = DeterministicScenarioCatalog.BaseTime;
        var expectedV1PosTs = t0.AddMinutes(8).ToUnixTimeMilliseconds();
        var expectedV2State = VehicleStates.Active;
        var expectedV2PosTs = t0.AddMinutes(12).ToUnixTimeMilliseconds();

        for (var i = 0; i < 60; i++)
        {
            var snap = await projector.SnapshotAllAsync(ct);

            var hasV1 = snap.TryGetValue(DeterministicScenarioCatalog.Vehicle1LifecycleAndPositions, out var v1)
                        && v1.State == VehicleStates.Decommissioned
                        && v1.PosRecordedAtUnixMs == expectedV1PosTs;

            var hasV2 = snap.TryGetValue(DeterministicScenarioCatalog.Vehicle2PositionBeforeVehicle, out var v2)
                        && v2.State == expectedV2State
                        && v2.PosRecordedAtUnixMs == expectedV2PosTs;

            var hasV3 = snap.TryGetValue(DeterministicScenarioCatalog.Vehicle3DeletedAndTombstoned, out var v3)
                        && v3.IsDeleted
                        && v3.State == VehicleStates.Deleted;

            if (hasV1 && hasV2 && hasV3)
            {
                return snap;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500), ct);
        }

        throw new TimeoutException("Valkey read model did not converge to expected state within 30 seconds.");
    }

    /// <summary>
    /// Flushes Valkey completely and replays both Kafka CDC topics from offset 0 through the
    /// exact same dispatcher/projector pipeline so replay determinism is verified end-to-end.
    /// </summary>
    public static async Task ReplayFromOffsetZeroDirectlyAsync(
        string kafkaBootstrap,
        ValkeyLuaProjector projector,
        CancellationToken ct)
    {
        await projector.FlushAllAsync(ct);

        var dispatcher = new VehicleCache.Infrastructure.Messaging.CdcEventDispatcher(
            projector,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<VehicleCache.Infrastructure.Messaging.CdcEventDispatcher>.Instance);

        // Replay both topics concurrently from Offset.Beginning (0) in a fresh replay consumer group
        var replayGroup = $"vehicle-cache-replay-{Guid.NewGuid():N}";

        await Task.WhenAll(
            DrainTopicFromZeroAsync(kafkaBootstrap, $"{replayGroup}-vehicles", "cdc.public.vehicles", dispatcher.HandleVehiclePayloadAsync, ct),
            DrainTopicFromZeroAsync(kafkaBootstrap, $"{replayGroup}-positions", "cdc.public.positions", dispatcher.HandlePositionPayloadAsync, ct));
    }

    private static async Task DrainTopicFromZeroAsync(
        string kafkaBootstrap,
        string groupId,
        string topic,
        Func<string?, CancellationToken, Task<ProjectionOutcome>> handler,
        CancellationToken ct)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = kafkaBootstrap,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnablePartitionEof = true
        };

        using var consumer = new ConsumerBuilder<Ignore, string?>(config).Build();
        consumer.Subscribe(topic);

        var emptyPolls = 0;
        while (!ct.IsCancellationRequested && emptyPolls < 6)
        {
            var res = consumer.Consume(TimeSpan.FromMilliseconds(500));
            if (res is null)
            {
                emptyPolls++;
                continue;
            }
            if (res.IsPartitionEOF)
            {
                break;
            }

            emptyPolls = 0;
            await handler(res.Message?.Value, ct);
        }

        consumer.Close();
    }

    public static void ValidateSnapshotAssertions(IReadOnlyDictionary<Guid, VehicleReadModel> snapshot, string label)
    {
        var t0 = DeterministicScenarioCatalog.BaseTime;

        var v1 = snapshot[DeterministicScenarioCatalog.Vehicle1LifecycleAndPositions];
        if (v1.State != VehicleStates.Decommissioned ||
            v1.StateUpdatedAtUnixMs != t0.AddMinutes(60).ToUnixTimeMilliseconds() ||
            v1.Latitude != -23.5725 ||
            v1.Longitude != -46.6512 ||
            v1.PosRecordedAtUnixMs != t0.AddMinutes(8).ToUnixTimeMilliseconds() ||
            v1.IsDeleted)
        {
            throw new InvalidOperationException($"{label} failed assertion on Vehicle 1: {JsonSerializer.Serialize(v1)}");
        }

        var v2 = snapshot[DeterministicScenarioCatalog.Vehicle2PositionBeforeVehicle];
        if (v2.State != VehicleStates.Active ||
            v2.Plate != "NST-2002" ||
            v2.Latitude != -25.4284 ||
            v2.Longitude != -49.2733 ||
            v2.PosRecordedAtUnixMs != t0.AddMinutes(12).ToUnixTimeMilliseconds() ||
            v2.IsDeleted)
        {
            throw new InvalidOperationException($"{label} failed assertion on Vehicle 2: {JsonSerializer.Serialize(v2)}");
        }

        var v3 = snapshot[DeterministicScenarioCatalog.Vehicle3DeletedAndTombstoned];
        if (!v3.IsDeleted ||
            v3.State != VehicleStates.Deleted ||
            v3.Latitude is not null ||
            v3.Longitude is not null)
        {
            throw new InvalidOperationException($"{label} failed assertion on Deleted Vehicle 3: {JsonSerializer.Serialize(v3)}");
        }

        Console.WriteLine($"  -> {label} verified: V1={v1.State}@({v1.Latitude},{v1.Longitude}), V2={v2.State}@({v2.Latitude},{v2.Longitude}), V3=DELETED(Tombstone LSN={v3.DeletedLsn})");
    }

    public static void AssertSnapshotsIdentical(
        IReadOnlyDictionary<Guid, VehicleReadModel> snapshotA,
        IReadOnlyDictionary<Guid, VehicleReadModel> snapshotB)
    {
        if (snapshotA.Count != snapshotB.Count)
        {
            throw new InvalidOperationException($"Snapshot key count mismatch: A={snapshotA.Count}, B={snapshotB.Count}");
        }

        foreach (var (vehicleId, modelA) in snapshotA)
        {
            if (!snapshotB.TryGetValue(vehicleId, out var modelB))
            {
                throw new InvalidOperationException($"Vehicle {vehicleId} missing in Snapshot B.");
            }

            if (!Equals(modelA, modelB))
            {
                throw new InvalidOperationException(
                    $"Snapshot mismatch for Vehicle {vehicleId}:\n  A: {JsonSerializer.Serialize(modelA)}\n  B: {JsonSerializer.Serialize(modelB)}");
            }
        }
    }
}
