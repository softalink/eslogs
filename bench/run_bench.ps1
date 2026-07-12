# Windows (MSVC) benchmark runner — mirror of run_bench.sh.
# Starts one EsLogs server, replays the shared corpus with esl-loadgen,
# measures CPU (GetProcessTimes via .NET TotalProcessorTime), peak RSS
# (PeakWorkingSet64), disk usage of the data dir, and query latency, then
# writes a result JSON compatible with compare.py.
#
# Usage:
#   pwsh run_bench.ps1 -Bin <server.exe> -Label rust -Corpus corpus.jsonl -Out rust.json `
#                      [-Port 9428] [-Rate 0] [-Conns 4]
param(
    [Parameter(Mandatory=$true)][string]$Bin,
    [Parameter(Mandatory=$true)][string]$Label,
    [Parameter(Mandatory=$true)][string]$Corpus,
    [Parameter(Mandatory=$true)][string]$Out,
    [int]$Port = 9428,
    [int]$Rate = 0,
    [int]$Conns = 4,
    [string]$IngestPath = '/insert/jsonline?_stream_fields=source&_msg_field=message&_time_field=@timestamp'
)
$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$loadgen = Join-Path $here 'loadgen/target/release/esl-loadgen.exe'
if (-not (Test-Path $loadgen)) { throw "build loadgen first: (cd bench/loadgen; cargo build --release)" }

$data = Join-Path $env:TEMP ("vlbench-$Label-" + [System.Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Force -Path $data | Out-Null
$log = "$data.server.log"

Write-Host "[$Label] starting $Bin (data=$data)"
$srv = Start-Process -FilePath $Bin `
    -ArgumentList @("-storageDataPath=$data", "-httpListenAddr=:$Port", "-retentionPeriod=10y") `
    -RedirectStandardOutput $log -RedirectStandardError "$log.err" -PassThru -NoNewWindow

try {
    # Wait for readiness.
    $ready = $false
    for ($i = 0; $i -lt 300; $i++) {
        try { Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$Port/health" -TimeoutSec 2 | Out-Null; $ready = $true; break }
        catch { if ($srv.HasExited) { throw "server died on startup: $(Get-Content $log,"$log.err" -Raw)" }; Start-Sleep -Milliseconds 100 }
    }
    if (-not $ready) { throw "server not ready" }

    Write-Host "[$Label] replaying corpus"
    $replay = & $loadgen replay --corpus $Corpus --host 127.0.0.1 --port $Port `
        --path $IngestPath --conns $Conns --rate $Rate --batch 1000 | Select-Object -Last 1

    try { Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$Port/internal/force_flush" -TimeoutSec 10 | Out-Null } catch {}
    Start-Sleep -Seconds 3
    try { Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$Port/internal/force_merge" -TimeoutSec 10 | Out-Null } catch {}
    Start-Sleep -Seconds 2

    $srv.Refresh()
    $cpuSeconds = [math]::Round($srv.TotalProcessorTime.TotalSeconds, 3)
    $peakRss = $srv.PeakWorkingSet64
    $diskBytes = (Get-ChildItem -Recurse -File $data | Measure-Object -Property Length -Sum).Sum
    if ($null -eq $diskBytes) { $diskBytes = 0 }

    function Measure-Query([string]$q) {
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        try {
            Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$Port/select/logsql/query" `
                -Method POST -Body @{ query = $q; limit = 100 } -TimeoutSec 30 | Out-Null
        } catch {}
        $sw.Stop(); [math]::Round($sw.Elapsed.TotalMilliseconds, 1)
    }
    $qAll = Measure-Query '*'
    $qPhrase = Measure-Query 'error'
    $qStats = Measure-Query '* | stats count() rows'

    $throughput = 0.0; $records = 0
    if ($replay -match '"throughput_rps":([0-9.]+)') { $throughput = [double]$Matches[1] }
    if ($replay -match '"records":([0-9]+)') { $records = [int]$Matches[1] }

    $result = [ordered]@{
        label = $Label; records = $records; throughput_rps = $throughput
        cpu_seconds = $cpuSeconds; peak_rss_bytes = $peakRss; disk_bytes = $diskBytes
        query_ms = [ordered]@{ match_all = $qAll; phrase = $qPhrase; stats_count = $qStats }
    }
    ($result | ConvertTo-Json -Depth 4) | Set-Content -Path $Out
    Write-Host "[$Label] done -> $Out"
    Get-Content $Out
}
finally {
    if (-not $srv.HasExited) { $srv.Kill() }
    Remove-Item -Recurse -Force $data, $log, "$log.err" -ErrorAction SilentlyContinue
}
