namespace VehicleCache.Infrastructure.Configuration;

public sealed class WorkerOptions
{
    public const string SectionName = "VehicleCache";

    public string KafkaBootstrapServers { get; set; } = "localhost:9092";
    public string ConsumerGroupId { get; set; } = "vehicle-cache-worker";
    public string VehiclesTopic { get; set; } = "cdc.public.vehicles";
    public string PositionsTopic { get; set; } = "cdc.public.positions";
    public string ValkeyConnectionString { get; set; } = "localhost:6379,abortConnect=false";
}
