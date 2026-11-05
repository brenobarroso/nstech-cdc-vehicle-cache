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
    devspace deploy --skip-push
    echo "Waiting for pods to become Ready..."
    kubectl wait --for=condition=available deployment --all --timeout=180s
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
    if (-not (kind get clusters | Select-String -SimpleMatch "{{cluster_name}}")) { kind create cluster --name "{{cluster_name}}" }
    kubectl config use-context "kind-{{cluster_name}}"
    devspace deploy --skip-push
    kubectl wait --for=condition=available deployment --all --timeout=180s
    Get-Process kubectl -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Process -WindowStyle Hidden kubectl -ArgumentList "port-forward svc/postgres 5432:5432"
    Start-Process -WindowStyle Hidden kubectl -ArgumentList "port-forward svc/valkey 6379:6379"
    Start-Process -WindowStyle Hidden kubectl -ArgumentList "port-forward svc/kafka 9092:9092"
    Start-Process -WindowStyle Hidden kubectl -ArgumentList "port-forward svc/debezium 8083:8083"
    Start-Sleep -Seconds 3
    Write-Host "Stack is UP and ready for 'just e2e'!"

# Run the complete verification suite: unit tests + live E2E scenarios (B1..B7) + Offset-0 Replay rebuild check (B8)
[unix]
e2e:
    dotnet test VehicleCache.sln --configuration Release
    dotnet run --project src/VehicleCache.MockWriters/VehicleCache.MockWriters.csproj --configuration Release

[windows]
e2e:
    $dotnet = if (Test-Path "$env:LOCALAPPDATA\Microsoft\dotnet\dotnet.exe") { "$env:LOCALAPPDATA\Microsoft\dotnet\dotnet.exe" } else { "dotnet" }; & $dotnet test VehicleCache.sln --configuration Release; & $dotnet run --project src/VehicleCache.MockWriters/VehicleCache.MockWriters.csproj --configuration Release

# Run fast deterministic unit tests (B1..B8 in-memory & parser verification)
[unix]
test:
    dotnet test VehicleCache.sln

[windows]
test:
    $dotnet = if (Test-Path "$env:LOCALAPPDATA\Microsoft\dotnet\dotnet.exe") { "$env:LOCALAPPDATA\Microsoft\dotnet\dotnet.exe" } else { "dotnet" }; & $dotnet test VehicleCache.sln

# Tear down the local Kubernetes cluster and stop port-forwards
[unix]
down:
    pkill -f "kubectl port-forward" || true
    kind delete cluster --name "{{cluster_name}}"

[windows]
down:
    Get-Process kubectl -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    kind delete cluster --name "{{cluster_name}}"
