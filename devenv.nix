{ pkgs, lib, config, ... }:

{
  env = {
    DOTNET_CLI_TELEMETRY_OPTOUT = "1";
    DOTNET_NOLOGO = "1";
    POSTGRES_CONNECTION_STRING = "Host=localhost;Port=5432;Database=logistics;Username=postgres;Password=postgres";
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    VALKEY_CONNECTION_STRING = "localhost:6379,abortConnect=false,allowAdmin=true";
    DEBEZIUM_CONNECT_URL = "http://localhost:8083";
  };

  packages = with pkgs; [
    dotnetCorePackages.sdk_10_0
    kubectl
    kubernetes-helm
    devspace
    just
    kind
    jq
    curl
    git
  ];

  enterShell = ''
    echo "================================================================"
    echo " NsTech CDC Vehicle Cache (.NET 10) - Reproducible DevEnv Shell"
    echo "================================================================"
    echo "  just up    -> Deploy Postgres, Kafka, Debezium, Valkey & Worker to local K8s"
    echo "  just e2e   -> Run full E2E scenarios (B1..B7) + Offset-0 Replay check (B8)"
    echo "  just test  -> Run unit & Valkey Lua integration test suite"
    echo "  just down  -> Tear down local K8s deployment"
    echo "================================================================"
  '';
}
