# Arranca un script de _runlogs (cola.ps1 o vigilar_airflow.ps1) si no esta ya corriendo. Lo usan las tareas
# programadas de Windows al iniciar sesion y cada 30 minutos; espera a que Docker responda antes de arrancar.
param([Parameter(Mandatory = $true)][string]$Script)
$Path = "D:\GitHub\gridpredic-infrastructure\_runlogs\$Script"
if (-not (Test-Path $Path)) { exit 1 }
$running = Get-CimInstance Win32_Process -Filter "Name='pwsh.exe' or Name='powershell.exe'" |
    Where-Object { $_.CommandLine -match [regex]::Escape($Script) -and $_.CommandLine -notmatch 'arrancar_si_falta' }
if ($running) { exit 0 }
for ($i = 0; $i -lt 60; $i++) {
    docker info *> $null
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep -Seconds 30
}
Start-Process pwsh -ArgumentList '-NoProfile', '-File', $Path -WindowStyle Hidden
