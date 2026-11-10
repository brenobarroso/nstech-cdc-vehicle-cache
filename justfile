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
    kubectl wait --for=condition=ready pod --all --timeout=240s
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
    $ErrorActionPreference = 'Continue'; cmd /c "docker info >nul 2>nul"; if ($LASTEXITCODE -ne 0) { Write-Host "Docker Desktop is closed — starting Docker Desktop automatically..."; Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = '"C:\Program Files\Docker\Docker\Docker Desktop.exe"' } | Out-Null; for ($i = 0; $i -lt 60; $i++) { Start-Sleep -Seconds 3; cmd /c "docker info >nul 2>nul"; if ($LASTEXITCODE -eq 0) { Write-Host "Docker Desktop is ready!"; break } } }; $kindCmd = Get-Command kind -ErrorAction SilentlyContinue; $kind = if ($kindCmd) { $kindCmd.Source } else { Get-ChildItem -Path "$env:LOCALAPPDATA\Microsoft\WinGet\Packages" -Filter "kind*.exe" -Recurse | Select-Object -First 1 -ExpandProperty FullName }; $devspaceCmd = Get-Command devspace -ErrorAction SilentlyContinue; $devspace = if ($devspaceCmd) { $devspaceCmd.Source } else { "$env:LOCALAPPDATA\Microsoft\WinGet\Links\devspace.exe" }; if (-not (& $kind get clusters | Select-String -SimpleMatch "{{cluster_name}}")) { & $kind create cluster --name "{{cluster_name}}" } else { cmd /c "docker start {{cluster_name}}-control-plane >nul 2>nul"; Start-Sleep -Seconds 5 }; kubectl config use-context "kind-{{cluster_name}}"; for ($i = 0; $i -lt 40; $i++) { cmd /c "kubectl get nodes >nul 2>nul"; if ($LASTEXITCODE -eq 0) { break }; Start-Sleep -Seconds 2 }; docker build -t vehicle-cache-worker:latest .; & $kind load docker-image vehicle-cache-worker:latest --name "{{cluster_name}}"; & $devspace deploy --skip-push; kubectl wait --for=condition=available deployment --all --timeout=240s; kubectl wait --for=condition=ready pod --all --timeout=240s; Get-Process kubectl -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue; Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = 'kubectl port-forward svc/postgres 5432:5432' } | Out-Null; Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = 'kubectl port-forward svc/valkey 6379:6379' } | Out-Null; Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = 'kubectl port-forward svc/kafka 9092:9092' } | Out-Null; Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = 'kubectl port-forward svc/debezium 8083:8083' } | Out-Null; Start-Sleep -Seconds 4; Write-Host "Stack is UP and ready for 'just e2e'!"

# Run the complete verification suite from a clean checkout or running cluster: ensures stack is up, runs unit tests + live E2E scenarios (B1..B7) + Offset-0 Replay rebuild check (B8)
[unix]
e2e:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! ((echo >/dev/tcp/127.0.0.1/5432) >/dev/null 2>&1 && (echo >/dev/tcp/127.0.0.1/6379) >/dev/null 2>&1 && (echo >/dev/tcp/127.0.0.1/9092) >/dev/null 2>&1 && (echo >/dev/tcp/127.0.0.1/8083) >/dev/null 2>&1); then
        echo "Stack not yet active on localhost ports — invoking 'just up' automatically..."
        just up
    fi
    dotnet test VehicleCache.sln --configuration Release
    dotnet run --project src/VehicleCache.MockWriters/VehicleCache.MockWriters.csproj --configuration Release

[windows]
e2e:
    $ErrorActionPreference = 'Stop'; $portsReady = $true; foreach ($p in @(5432, 6379, 9092, 8083)) { $c = New-Object System.Net.Sockets.TcpClient; try { $c.Connect('127.0.0.1', $p) } catch { $portsReady = $false } finally { $c.Dispose() } }; if (-not $portsReady) { Write-Host "Stack not yet active on localhost ports — invoking 'just up' automatically..."; just up }; $dotnet = if (Test-Path "$env:LOCALAPPDATA\Microsoft\dotnet\dotnet.exe") { "$env:LOCALAPPDATA\Microsoft\dotnet\dotnet.exe" } else { "dotnet" }; $noBuild = if (Test-Path "$env:LOCALAPPDATA\VehicleCacheBuild\bin\VehicleCache.MockWriters\Release\net10.0\VehicleCache.MockWriters.dll") { "--no-build" } else { "" }; & $dotnet test VehicleCache.sln --configuration Release $noBuild; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }; & $dotnet run --project src/VehicleCache.MockWriters/VehicleCache.MockWriters.csproj --configuration Release $noBuild; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# Run fast deterministic unit tests (B1..B8 in-memory & parser verification)
[unix]
test:
    dotnet test VehicleCache.sln

[windows]
test:
    $ErrorActionPreference = 'Stop'; $dotnet = if (Test-Path "$env:LOCALAPPDATA\Microsoft\dotnet\dotnet.exe") { "$env:LOCALAPPDATA\Microsoft\dotnet\dotnet.exe" } else { "dotnet" }; $noBuild = if (Test-Path "$env:LOCALAPPDATA\VehicleCacheBuild\bin\VehicleCache.Tests\Debug\net10.0\VehicleCache.Tests.dll") { "--no-build" } else { "" }; & $dotnet test VehicleCache.sln $noBuild; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# Tear down the local Kubernetes cluster and stop port-forwards
[unix]
down:
    pkill -f "kubectl port-forward" || true
    kind delete cluster --name "{{cluster_name}}"

[windows]
down:
    $kindCmd = Get-Command kind -ErrorAction SilentlyContinue; $kind = if ($kindCmd) { $kindCmd.Source } else { Get-ChildItem -Path "$env:LOCALAPPDATA\Microsoft\WinGet\Packages" -Filter "kind*.exe" -Recurse | Select-Object -First 1 -ExpandProperty FullName }; Get-Process kubectl -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue; & $kind delete cluster --name "{{cluster_name}}"
