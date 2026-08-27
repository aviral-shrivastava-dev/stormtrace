# Removes the StormTrace scheduled task.
# Run from PowerShell:
#   powershell -ExecutionPolicy Bypass -File scripts\remove_scheduler.ps1

$taskName = "StormTracePipeline"

$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($null -eq $existing) {
    Write-Output "No scheduled task named '$taskName' exists."
    exit 0
}

Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
Write-Output "Removed scheduled task '$taskName'."
Write-Output "Run scripts\setup_scheduler.ps1 to enable automation again."
