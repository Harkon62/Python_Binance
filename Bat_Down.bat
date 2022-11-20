@echo off  & setlocal
echo %1
SET paraHour = %1
ECHO %paraHour%
REM if "%paraHour%" =="" (echo "72") else (echo "leer")
REM
if "%paraHour%" =="" (cmd /k python3.10 main.py 144) else (cmd /k python3.10 main.py %paraHour%)
PAUSE
