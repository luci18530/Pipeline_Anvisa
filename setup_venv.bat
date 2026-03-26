@echo off
setlocal

set SCRIPT_DIR=%~dp0
powershell -ExecutionPolicy Bypass -File "%SCRIPT_DIR%setup_venv.ps1"

if errorlevel 1 (
    echo.
    echo [ERRO] Falha no setup do ambiente.
    exit /b 1
)

echo.
echo [OK] Setup finalizado.
exit /b 0
