param(
  [string]$HostName = $env:HOST,
  [int]$Port = $(if ($env:PORT) { [int]$env:PORT } else { 8787 }),
  [string]$DataDir = $env:DATA_DIR
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if (-not $HostName) { $HostName = "0.0.0.0" }
if (-not $DataDir) { $DataDir = Join-Path $root "data" }

$releaseExe = Join-Path $root "target\release\failover-proxy.exe"
$debugExe = Join-Path $root "target\debug\failover-proxy.exe"
$embeddedIndex = Join-Path $root "assets\index.html"
$uiInputs = @(
  (Join-Path $root "ui\src"),
  (Join-Path $root "ui\index.html"),
  (Join-Path $root "ui\package.json"),
  (Join-Path $root "ui\package-lock.json"),
  (Join-Path $root "ui\vite.config.ts"),
  (Join-Path $root "scripts\copy-ui.mjs")
)
$inputs = @(
  (Join-Path $root "Cargo.toml"),
  (Join-Path $root "Cargo.lock"),
  (Join-Path $root "src"),
  (Join-Path $root "assets")
)

function Test-NeedsBuild {
  param([string]$ExePath, [string[]]$InputPaths)
  if (-not (Test-Path -LiteralPath $ExePath)) { return $true }
  $exeTime = (Get-Item -LiteralPath $ExePath).LastWriteTimeUtc
  foreach ($inputPath in $InputPaths) {
    if (-not (Test-Path -LiteralPath $inputPath)) { continue }
    $item = Get-Item -LiteralPath $inputPath
    if ($item.PSIsContainer) {
      $newer = Get-ChildItem -LiteralPath $inputPath -Recurse -File |
        Where-Object { $_.LastWriteTimeUtc -gt $exeTime } |
        Select-Object -First 1
      if ($newer) { return $true }
    } elseif ($item.LastWriteTimeUtc -gt $exeTime) {
      return $true
    }
  }
  return $false
}

if (Test-NeedsBuild -ExePath $embeddedIndex -InputPaths $uiInputs) {
  $npm = Get-Command npm -ErrorAction SilentlyContinue
  if (-not $npm) {
    throw "Frontend sources are newer than embedded assets, but npm was not found. Install Node.js or run npm run build:ui first."
  }
  Write-Host "Building and embedding the admin UI..."
  & npm run build:ui
  if ($LASTEXITCODE -ne 0) {
    throw "Frontend build failed (npm run build:ui)."
  }
}

if (Test-NeedsBuild -ExePath $releaseExe -InputPaths $inputs) {
  $cargo = Get-Command cargo -ErrorAction SilentlyContinue
  if ($cargo) {
    Write-Host "Building Failover Proxy release binary..."
    cargo build --release --offline
  } elseif (-not (Test-Path -LiteralPath $debugExe)) {
    throw "Cargo was not found and no existing Failover Proxy binary is available."
  }
}

$exe = if (Test-Path -LiteralPath $releaseExe) { $releaseExe } else { $debugExe }
$env:HOST = $HostName
$env:PORT = [string]$Port
$env:DATA_DIR = $DataDir
if (-not $env:RUST_LOG) { $env:RUST_LOG = "failover_proxy=info,tower_http=info" }

Write-Host "Failover Proxy will listen on http://$HostName`:$Port"
Write-Host "Admin UI: http://127.0.0.1:$Port"
Write-Host "Data dir: $DataDir"

Start-Job -ScriptBlock {
  param($AdminUrl)
  Start-Sleep -Seconds 2
  Start-Process $AdminUrl
} -ArgumentList "http://127.0.0.1:$Port" | Out-Null

& $exe --host $HostName --port $Port --data-dir $DataDir
