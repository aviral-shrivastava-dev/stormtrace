# Registers the StormTrace hourly Windows scheduled task.
# Run from PowerShell:
#   powershell -ExecutionPolicy Bypass -File scripts\setup_scheduler.ps1

$ErrorActionPreference = "Stop"

$taskName = "StormTracePipeline"
$scriptsDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptsDir
$batchPath = Join-Path $scriptsDir "run_scheduled.bat"

if (-not (Test-Path $batchPath)) {
    Write-Error "Wrapper not found: $batchPath"
    exit 1
}

$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$batchPath`""
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(2) `
    -RepetitionInterval (New-TimeSpan -Hours 1) `
    -RepetitionDuration (New-TimeSpan -Days 3650)
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries

try {
    Register-ScheduledTask -TaskName $taskName -Action $action `
        -Trigger $trigger -Settings $settings -Force | Out-Null
    Write-Output "Registered scheduled task '$taskName'."
    Write-Output "It runs every hour, starting in about 2 minutes."
    Write-Output "The pipeline skips network downloads younger than 2 hours,"
    Write-Output "so CelesTrak and NOAA are never asked too often."
}
catch {
    Write-Output "Register-ScheduledTask failed: $($_.Exception.Message)"
    Write-Output "Falling back to schtasks.exe..."
    schtasks.exe /Create /F /TN $taskName /SC HOURLY /TR "`"$batchPath`""
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Could not register the scheduled task."
        exit 1
    }
    Write-Output "Registered via schtasks (hourly, without catch-up option)."
}

Write-Output ""
Write-Output "Check status:  schtasks /Query /TN $taskName /V /FO LIST"
Write-Output "Force one run: schtasks /Run /TN $taskName"
Write-Output "Remove task:   powershell -ExecutionPolicy Bypass -File scripts\remove_scheduler.ps1"
Write-Output "Last run log:  data\logs\scheduler_last_run.log"
