@echo off
rem StormTrace scheduled-task wrapper.
rem Task Scheduler runs this file every hour. The pipeline itself decides
rem whether network downloads are due, so an hourly trigger is safe.
rem %~dp0 expands to this script's own directory, so the project can live
rem anywhere without editing this file.
cd /d "%~dp0.."
if not exist "data\logs" mkdir "data\logs"
python "src\run_pipeline.py" > "data\logs\scheduler_last_run.log" 2>&1
exit /b %ERRORLEVEL%
