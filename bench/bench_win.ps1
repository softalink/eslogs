# Windows benchmark: Rust vs Go EsLogs, single-ingest 500k corpus.
# Runs in $env:ESLOGS_BENCH_DIR (default C:\eslogs-bench) with the *.exe
# binaries and corpus_big.jsonl present.
$ErrorActionPreference = "Stop"
$base = if ($env:ESLOGS_BENCH_DIR) { $env:ESLOGS_BENCH_DIR } else { "C:\eslogs-bench" }
$corpus = "$base\corpus_big.jsonl"
$loadgen = "$base\esl-loadgen.exe"

function Run-One($exe, $port, $data, $label) {
    Remove-Item -Recurse -Force $data -ErrorAction SilentlyContinue
    $a = @("-storageDataPath=$data", "-httpListenAddr=127.0.0.1:$port", "-retentionPeriod=10y")
    $p = Start-Process -FilePath $exe -ArgumentList $a -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput "$data.out.log" -RedirectStandardError "$data.err.log"
    $ready = $false
    for ($i = 0; $i -lt 60; $i++) {
        try { Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$port/health" -TimeoutSec 2 | Out-Null; $ready = $true; break }
        catch { Start-Sleep -Milliseconds 500 }
    }
    if (-not $ready) { Write-Output "$label ERROR not ready"; try { Stop-Process -Id $p.Id -Force } catch {}; return }

    $ingest = '/insert/jsonline?_stream_fields=source&_msg_field=message&_time_field=@timestamp'
    $rep = & $loadgen replay --corpus $corpus --host 127.0.0.1 --port $port --path $ingest --conns 8 --batch 2000 | Select-Object -Last 1
    $tput = 0.0; if ($rep -match '"throughput_rps":([0-9.]+)') { $tput = [double]$Matches[1] }

    try { Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$port/internal/force_flush" -TimeoutSec 60 | Out-Null } catch {}
    Start-Sleep 3
    try { Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$port/internal/force_merge" -TimeoutSec 60 | Out-Null } catch {}
    Start-Sleep 4

    function Qmin($q) {
        # Time with curl.exe (like the Linux harness) — PowerShell's
        # Invoke-WebRequest has high per-call overhead that dwarfs the query.
        # limit=100 matches run_bench.sh.
        $best = 1e9
        $url = "http://127.0.0.1:$port/select/logsql/query"
        for ($i = 0; $i -lt 6; $i++) {
            $ms = (Measure-Command { & curl.exe -s -o NUL --data-urlencode "query=$q" --data-urlencode "limit=100" $url }).TotalMilliseconds
            if ($ms -lt $best) { $best = $ms }
        }
        return [math]::Round($best, 1)
    }
    $qAll = Qmin '*'
    $qPhrase = Qmin 'error'
    $qStats = Qmin '* | stats count() rows'

    $p.Refresh()
    $cpu = [math]::Round($p.TotalProcessorTime.TotalSeconds, 3)
    $rssMb = [math]::Round($p.PeakWorkingSet64 / 1MB, 1)
    $disk = (Get-ChildItem -Recurse -File $data -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum
    if ($null -eq $disk) { $disk = 0 }
    Stop-Process -Id $p.Id -Force
    Start-Sleep 1
    Write-Output ("{0}: tput={1} cpu={2}s rss={3}MB disk={4} q_all={5}ms q_phrase={6}ms q_stats={7}ms" -f $label, $tput, $cpu, $rssMb, $disk, $qAll, $qPhrase, $qStats)
    return [pscustomobject]@{ tput=$tput; cpu=$cpu; rss=$rssMb; disk=$disk; q_all=$qAll; q_phrase=$qPhrase; q_stats=$qStats }
}

function Median($vals) {
    $s = $vals | Sort-Object
    return $s[[math]::Floor(($s.Count - 1) / 2)]
}

# Ingest throughput and CPU are within a few percent between the two servers,
# so a single run flips on scheduler/thermal noise. Run 3 alternating
# iterations and compare medians.
Write-Output "=== Windows 500k benchmark (median of 5 alternating runs) ==="
$r = @(); $g = @()
for ($iter = 1; $iter -le 5; $iter++) {
    Write-Output "--- iteration $iter ---"
    $r += Run-One "$base\es-logs.exe" 9538 "$base\dr" "RUST"
    $g += Run-One "$base\victoria-logs-go.exe" 9539 "$base\dg" "GO"
}
Write-Output "=== medians ==="
Write-Output ("RUST-med: tput={0} cpu={1}s rss={2}MB disk={3} q_all={4}ms q_phrase={5}ms q_stats={6}ms" -f `
    (Median $r.tput), (Median $r.cpu), (Median $r.rss), (Median $r.disk), (Median $r.q_all), (Median $r.q_phrase), (Median $r.q_stats))
Write-Output ("GO-med:   tput={0} cpu={1}s rss={2}MB disk={3} q_all={4}ms q_phrase={5}ms q_stats={6}ms" -f `
    (Median $g.tput), (Median $g.cpu), (Median $g.rss), (Median $g.disk), (Median $g.q_all), (Median $g.q_phrase), (Median $g.q_stats))
