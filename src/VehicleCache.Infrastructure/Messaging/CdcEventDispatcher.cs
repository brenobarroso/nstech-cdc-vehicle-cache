using Microsoft.Extensions.Logging;
using VehicleCache.Core.Abstractions;
using VehicleCache.Core.Domain;
using VehicleCache.Core.Parsing;

namespace VehicleCache.Infrastructure.Messaging;

/// <summary>
/// Parses raw Kafka CDC payloads from Debezium and dispatches them to <see cref="IVehicleReadModelProjector"/>.
/// Tracks projection outcome metrics for observability.
/// </summary>
public sealed class CdcEventDispatcher
{
    private readonly IVehicleReadModelProjector _projector;
    private readonly ILogger<CdcEventDispatcher> _logger;

    private long _appliedCount;
    private long _skippedStaleCount;
    private long _skippedDuplicateCount;
    private long _skippedTombstonedCount;
    private long _ignoredKafkaTombstoneCount;

    public CdcEventDispatcher(IVehicleReadModelProjector projector, ILogger<CdcEventDispatcher> logger)
    {
        _projector = projector ?? throw new ArgumentNullException(nameof(projector));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public long AppliedCount => Interlocked.Read(ref _appliedCount);
    public long SkippedStaleCount => Interlocked.Read(ref _skippedStaleCount);
    public long SkippedDuplicateCount => Interlocked.Read(ref _skippedDuplicateCount);
    public long SkippedTombstonedCount => Interlocked.Read(ref _skippedTombstonedCount);
    public long IgnoredKafkaTombstoneCount => Interlocked.Read(ref _ignoredKafkaTombstoneCount);

    public async Task<ProjectionOutcome> HandleVehiclePayloadAsync(string? rawJson, CancellationToken ct = default)
    {
        var evt = DebeziumEnvelopeParser.ParseVehicleEvent(rawJson);
        if (evt is null)
        {
            Interlocked.Increment(ref _ignoredKafkaTombstoneCount);
            _logger.LogDebug("Ignored Kafka null/compaction tombstone on vehicles topic.");
            return ProjectionOutcome.IgnoredKafkaTombstone;
        }

        var outcome = await _projector.ProjectVehicleAsync(evt, ct).ConfigureAwait(false);
        RecordMetric(outcome);

        _logger.LogDebug(
            "Vehicle CDC event [VehicleId={VehicleId}, Op={Op}, State={State}, Lsn={Lsn}] -> {Outcome}",
            evt.VehicleId, evt.Operation, evt.State, evt.Lsn, outcome);

        return outcome;
    }

    public async Task<ProjectionOutcome> HandlePositionPayloadAsync(string? rawJson, CancellationToken ct = default)
    {
        var evt = DebeziumEnvelopeParser.ParsePositionEvent(rawJson);
        if (evt is null)
        {
            Interlocked.Increment(ref _ignoredKafkaTombstoneCount);
            _logger.LogDebug("Ignored Kafka null/compaction tombstone on positions topic.");
            return ProjectionOutcome.IgnoredKafkaTombstone;
        }

        var outcome = await _projector.ProjectPositionAsync(evt, ct).ConfigureAwait(false);
        RecordMetric(outcome);

        _logger.LogDebug(
            "Position CDC event [VehicleId={VehicleId}, Op={Op}, RecordedAtMs={RecordedAt}, Lsn={Lsn}] -> {Outcome}",
            evt.VehicleId, evt.Operation, evt.RecordedAtUnixMs, evt.Lsn, outcome);

        return outcome;
    }

    private void RecordMetric(ProjectionOutcome outcome)
    {
        switch (outcome)
        {
            case ProjectionOutcome.Applied:
                Interlocked.Increment(ref _appliedCount);
                break;
            case ProjectionOutcome.SkippedStale:
                Interlocked.Increment(ref _skippedStaleCount);
                break;
            case ProjectionOutcome.SkippedDuplicate:
                Interlocked.Increment(ref _skippedDuplicateCount);
                break;
            case ProjectionOutcome.SkippedTombstoned:
                Interlocked.Increment(ref _skippedTombstonedCount);
                break;
            case ProjectionOutcome.IgnoredKafkaTombstone:
                Interlocked.Increment(ref _ignoredKafkaTombstoneCount);
                break;
        }
    }
}
