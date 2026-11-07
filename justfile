set shell := ["bash", "-cu"]
set windows-shell := ["powershell.exe", "-NoLogo", "-NoProfile", "-Command"]

cluster_name := "nstech-cdc-cache"

# Default recipe: list available workflows
default:
    @just --list

# Bring the full stack up on local Kubernetes (kind + devspace) and wait for readiness
[unix]
up:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! kind get clusters | grep -q "^{{cluster_name}}$"; then
        echo "Creating local Kubernetes cluster '{{cluster_name}}' via kind..."
        kind create cluster --name "{{cluster_name}}"
    fi
    kubectl config use-context "kind-{{cluster_name}}"
    echo "Building .NET 10 Worker image and loading into kind cluster..."
    docker build -t vehicle-cache-worker:latest .
    kind load docker-image vehicle-cache-worker:latest --name "{{cluster_name}}"
    devspace deploy --skip-push
    echo "Waiting for pods to become Ready..."
    kubectl wait --for=condition=available deployment --all --timeout=240s
    echo "Starting background port-forwards for Postgres (5432), Valkey (6379), Kafka (9092), and Debezium (8083)..."
    pkill -f "kubectl port-forward" || true
    kubectl port-forward svc/postgres 5432:5432 >/dev/null 2>&1 &
    kubectl port-forward svc/valkey 6379:6379 >/dev/null 2>&1 &
    kubectl port-forward svc/kafka 9092:9092 >/dev/null 2>&1 &
    kubectl port-forward svc/debezium 8083:8083 >/dev/null 2>&1 &
    sleep 3
    echo "Stack is UP and ready for 'just e2e'!"

[windows]
up:
    $ErrorActionPreference = 'Stop'; $kindCmd = Get-Command kind -ErrorAction SilentlyContinue; $kind = if ($kindCmd) { $kindCmd.Source } else { Get-ChildItem -Path "$env:LOCALAPPDATA\Microsoft\WinGet\Packages" -Filter "kind*.exe" -Recurse | Select-Object -First 1 -ExpandProperty FullName }; $devspaceCmd = Get-Command devspace -ErrorAction SilentlyContinue; $devspace = if ($devspaceCmd) { $devspaceCmd.Source } else { "$env:LOCALAPPDATA\Microsoft\WinGet\Links\devspace.exe" }; if (-not (& $kind get clusters | Select-String -SimpleMatch "{{cluster_name}}")) { & $kind create cluster --name "{{cluster_name}}" }; kubectl config use-context "kind-{{cluster_name}}"; docker build -t vehicle-cache-worker:latest .; & $kind load docker-image vehicle-cache-worker:latest --name "{{cluster_name}}"; & $devspace deploy --skip-push; kubectl wait --for=condition=available deployment --all --timeout=240s; Get-Process kubectl -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue; Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = 'kubectl port-forward svc/postgres 5432:5432' } | Out-Null; Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = 'kubectl port-forward svc/valkey 6379:6379' } | Out-Null; Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = 'kubectl port-forward svc/kafka 9092:9092' } | Out-Null; Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = 'kubectl port-forward svc/debezium 8083:8083' } | Out-Null; Start-Sleep -Seconds 3; Write-Host "Stack is UP and ready for 'just e2e'!"

# Run the complete verification suite: unit tests + live E2E scenarios (B1..B7) + Offset-0 Replay rebuild check (B8)
[unix]
e2e:
    dotnet test VehicleCache.sln --configuration Release
    dotnet run --project src/VehicleCache.MockWriters/VehicleCache.MockWriters.csproj --configuration Release

[windows]
e2e:
    $ErrorActionPreference = 'Stop'; $dotnet = if (Test-Path "$env:LOCALAPPDATA\Microsoft\dotnet\dotnet.exe") { "$env:LOCALAPPDATA\Microsoft\dotnet\dotnet.exe" } else { "dotnet" }; & $dotnet test VehicleCache.sln --configuration Release; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }; & $dotnet run --project src/VehicleCache.MockWriters/VehicleCache.MockWriters.csproj --configuration Release; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# Run fast deterministic unit tests (B1..B8 in-memory & parser verification)
[unix]
test:
    dotnet test VehicleCache.sln

[windows]
test:
    $ErrorActionPreference = 'Stop'; $dotnet = if (Test-Path "$env:LOCALAPPDATA\Microsoft\dotnet\dotnet.exe") { "$env:LOCALAPPDATA\Microsoft\dotnet\dotnet.exe" } else { "dotnet" }; & $dotnet test VehicleCache.sln; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# Tear down the local Kubernetes cluster and stop port-forwards
[unix]
down:
    pkill -f "kubectl port-forward" || true
    kind delete cluster --name "{{cluster_name}}"

[windows]
down:
    $kindCmd = Get-Command kind -ErrorAction SilentlyContinue; $kind = if ($kindCmd) { $kindCmd.Source } else { Get-ChildItem -Path "$env:LOCALAPPDATA\Microsoft\WinGet\Packages" -Filter "kind*.exe" -Recurse | Select-Object -First 1 -ExpandProperty FullName }; Get-Process kubectl -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue; & $kind delete cluster --name "{{cluster_name}}"
