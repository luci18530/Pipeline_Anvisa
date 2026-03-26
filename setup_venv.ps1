param(
    [string]$VenvPath = ".venv",
    [string]$PythonCmd = "python",
    [switch]$Dev
)

$ErrorActionPreference = "Stop"

# Garante execucao relativa ao diretorio do script (raiz do repo)
$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $RepoRoot

Write-Host "=" * 70
Write-Host "SETUP DO AMBIENTE PYTHON (VENV + REQUIREMENTS)"
Write-Host "=" * 70
Write-Host "[INFO] Repositorio: $RepoRoot"
Write-Host "[INFO] Venv alvo:   $VenvPath"

# Verifica se o Python esta disponivel
if (-not (Get-Command $PythonCmd -ErrorAction SilentlyContinue)) {
    throw "Python nao encontrado no PATH. Instale o Python 3 e tente novamente."
}

Write-Host "[1/4] Criando ambiente virtual..."
& $PythonCmd -m venv $VenvPath

$VenvPython = Join-Path $RepoRoot "$VenvPath\Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    throw "Falha ao criar venv. Nao foi encontrado: $VenvPython"
}

Write-Host "[2/4] Atualizando pip, setuptools e wheel..."
& $VenvPython -m pip install --upgrade pip setuptools wheel

$Requirements = Join-Path $RepoRoot "requirements.txt"
if (-not (Test-Path $Requirements)) {
    throw "Arquivo requirements.txt nao encontrado em $RepoRoot"
}

Write-Host "[3/4] Instalando dependencias de requirements.txt..."
& $VenvPython -m pip install -r $Requirements

if ($Dev) {
    $DevRequirements = Join-Path $RepoRoot "requirements-dev.txt"
    if (Test-Path $DevRequirements) {
        Write-Host "[4/4] Instalando dependencias de desenvolvimento..."
        & $VenvPython -m pip install -r $DevRequirements
    }
    else {
        Write-Host "[4/4] requirements-dev.txt nao encontrado. Etapa ignorada."
    }
}
else {
    Write-Host "[4/4] Dependencias de desenvolvimento nao solicitadas."
}

$ActivateCmd = Join-Path $RepoRoot "$VenvPath\Scripts\Activate.ps1"

Write-Host ""
Write-Host "[OK] Setup concluido com sucesso!"
Write-Host "Para ativar o ambiente, execute:"
Write-Host "  $ActivateCmd"
Write-Host ""
