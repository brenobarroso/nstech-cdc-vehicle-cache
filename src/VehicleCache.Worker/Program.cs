using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using VehicleCache.Core.Abstractions;
using VehicleCache.Infrastructure.Configuration;
using VehicleCache.Infrastructure.Messaging;
using VehicleCache.Infrastructure.Projection;
using VehicleCache.Worker;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<WorkerOptions>(
    builder.Configuration.GetSection(WorkerOptions.SectionName));

builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
{
    var opts = sp.GetRequiredService<IOptions<WorkerOptions>>().Value;
    return ConnectionMultiplexer.Connect(opts.ValkeyConnectionString);
});

builder.Services.AddSingleton<IVehicleReadModelProjector, ValkeyLuaProjector>();
builder.Services.AddSingleton<CdcEventDispatcher>();
builder.Services.AddHostedService<CdcProjectionWorker>();

var host = builder.Build();
await host.RunAsync();
