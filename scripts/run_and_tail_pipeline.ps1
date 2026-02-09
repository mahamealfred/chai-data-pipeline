<#
.SYNOPSIS
Run `docker-compose up --build -d pipeline` then tail service logs.

.DESCRIPTION
This wrapper runs the exact command you requested to start the `pipeline`
service and then streams logs from the pipeline and its related services so
you can watch how the pipeline runs end-to-end.

.EXAMPLE
    .\scripts\run_and_tail_pipeline.ps1
#>

param(
    [string[]]$ServicesToTail = @('pipeline','postgres','minio','airflow','postgres-airflow'),
    [int]$TailLines = 200
)

function Ensure-Command {
    param([string]$Cmd)
    $null = Get-Command $Cmd -ErrorAction SilentlyContinue
    if ($?) { return $true }
    Write-Error "Required command '$Cmd' not found. Install Docker and docker-compose."
    return $false
}

if (-not (Ensure-Command -Cmd 'docker-compose')) { exit 2 }

Write-Host "Running: docker-compose up --build -d pipeline" -ForegroundColor Cyan
try {
    & docker-compose up --build -d pipeline
    if ($LASTEXITCODE -ne 0) { Write-Error "docker-compose exited with code $LASTEXITCODE"; exit $LASTEXITCODE }
} catch {
    Write-Error "Failed to start pipeline service: $($_.Exception.Message)"; exit 1
}

Write-Host "Streaming logs for: $($ServicesToTail -join ', ') (press Ctrl+C to stop)" -ForegroundColor Cyan
try {
    & docker-compose logs -f --no-color --tail=$TailLines @ServicesToTail
} catch {
    Write-Error "Failed to tail logs: $($_.Exception.Message)"; exit 1
}

Write-Host "Done." -ForegroundColor Green
