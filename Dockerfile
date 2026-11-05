FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Directory.Build.props ./
COPY VehicleCache.sln ./
COPY src/VehicleCache.Core/VehicleCache.Core.csproj src/VehicleCache.Core/
COPY src/VehicleCache.Infrastructure/VehicleCache.Infrastructure.csproj src/VehicleCache.Infrastructure/
COPY src/VehicleCache.Worker/VehicleCache.Worker.csproj src/VehicleCache.Worker/
COPY src/VehicleCache.MockWriters/VehicleCache.MockWriters.csproj src/VehicleCache.MockWriters/
COPY tests/VehicleCache.Tests/VehicleCache.Tests.csproj tests/VehicleCache.Tests/

RUN dotnet restore VehicleCache.sln

COPY src/ src/
RUN dotnet publish src/VehicleCache.Worker/VehicleCache.Worker.csproj -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .

ENV VehicleCache__KafkaBootstrapServers="kafka:29092" \
    VehicleCache__ValkeyConnectionString="valkey:6379,abortConnect=false,allowAdmin=true" \
    VehicleCache__VehiclesTopic="cdc.public.vehicles" \
    VehicleCache__PositionsTopic="cdc.public.positions"

ENTRYPOINT ["dotnet", "VehicleCache.Worker.dll"]
