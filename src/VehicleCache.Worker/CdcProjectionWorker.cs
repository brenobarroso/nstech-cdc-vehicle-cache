using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using VehicleCache.Infrastructure.Configuration;
using VehicleCache.Infrastructure.Messaging;

namespace VehicleCache.Worker;

/// <summary>
/// .NET 10 BackgroundService that consumes both Debezium CDC topics (<c>cdc.public.vehicles</c> and
/// <c>cdc.public.positions</c>) with manual offset commits after atomic Valkey projection.
/// Never queries PostgreSQL directly.
/// </summary>
public sealed class CdcProjectionWorker : BackgroundService
{
    private readonly WorkerOptions _options;
    private readonly CdcEventDispatcher _dispatcher;
    private readonly ILogger<CdcProjectionWorker> _logger;

    public CdcProjectionWorker(
        IOptions<WorkerOptions> options,
        CdcEventDispatcher dispatcher,
        ILogger<CdcProjectionWorker> logger)
    {
        _options = options.Value;
        _dispatcher = dispatcher;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Starting CdcProjectionWorker [Bootstrap={Bootstrap}, Group={Group}, VehiclesTopic={VehiclesTopic}, PositionsTopic={PositionsTopic}]",
            _options.KafkaBootstrapServers,
            _options.ConsumerGroupId,
            _options.VehiclesTopic,
            _options.PositionsTopic);

        // Run independent consumer loops for vehicles and positions concurrently.
        // Cross-stream race safety is guaranteed by Valkey server-side Lua scripts.
        var vehiclesTask = Task.Run(() => ConsumeTopicLoopAsync(
            topic: _options.VehiclesTopic,
            groupId: $"{_options.ConsumerGroupId}-vehicles",
            handler: _dispatcher.HandleVehiclePayloadAsync,
            stoppingToken), stoppingToken);

        var positionsTask = Task.Run(() => ConsumeTopicLoopAsync(
            topic: _options.PositionsTopic,
            groupId: $"{_options.ConsumerGroupId}-positions",
            handler: _dispatcher.HandlePositionPayloadAsync,
            stoppingToken), stoppingToken);

        await Task.WhenAll(vehiclesTask, positionsTask).ConfigureAwait(false);
    }

    private async Task ConsumeTopicLoopAsync(
        string topic,
        string groupId,
        Func<string?, CancellationToken, Task<Core.Domain.ProjectionOutcome>> handler,
        CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.KafkaBootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AllowAutoCreateTopics = true,
            SessionTimeoutMs = 10000,
            MaxPollIntervalMs = 300000
        };

        using var consumer = new ConsumerBuilder<Ignore, string?>(config)
            .SetErrorHandler((_, err) => _logger.LogWarning("Kafka consumer warning on {Topic}: {Reason}", topic, err.Reason))
            .Build();

        consumer.Subscribe(topic);
        _logger.LogInformation("Subscribed to CDC topic {Topic} (GroupId={GroupId})", topic, groupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(TimeSpan.FromMilliseconds(250));
                    if (result is null || result.IsPartitionEOF)
                    {
                        continue;
                    }

                    await handler(result.Message?.Value, stoppingToken).ConfigureAwait(false);
                    consumer.Commit(result);
                }
                catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
                {
                    _logger.LogDebug("Waiting for Debezium to create topic {Topic}...", topic);
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing CDC message on topic {Topic}; retrying shortly.", topic);
                    await Task.Delay(TimeSpan.FromMilliseconds(500), stoppingToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            try
            {
                consumer.Close();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error closing consumer for topic {Topic}.", topic);
            }
        }
    }
}
