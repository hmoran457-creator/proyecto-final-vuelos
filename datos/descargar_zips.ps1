# descargar_zips.ps1
# Descarga los 12 zips mensuales de 2024 del BTS (On-Time Reporting) a la carpeta "zips"
# que esta junto a este script. Funciona con Windows PowerShell 5.1.
#
# Uso:
#   .\descargar_zips.ps1               descarga los meses que falten (12 archivos, unos 360 MB)
#   .\descargar_zips.ps1 -SoloListar   solo imprime las 12 ligas, no descarga nada
#
# Si PowerShell dice que la ejecucion de scripts esta deshabilitada, corre una sola vez en esa
# misma ventana:  Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

param(
    [switch]$SoloListar
)

$anio = 2024
$patronUrl = "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{0}_{1}.zip"
$carpeta = Join-Path $PSScriptRoot "zips"
$minimoBytes = 1000000

if ($SoloListar) {
    foreach ($mes in 1..12) {
        $url = [string]::Format($patronUrl, $anio, $mes)
        Write-Host ("mes {0:00}: {1}" -f $mes, $url)
    }
    return
}

if (-not (Test-Path $carpeta)) {
    New-Item -ItemType Directory -Path $carpeta | Out-Null
}

# Windows PowerShell 5.1 a veces no negocia TLS 1.2 por defecto.
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$descargados = 0
$yaEstaban = 0
$fallaron = @()

foreach ($mes in 1..12) {
    $url = [string]::Format($patronUrl, $anio, $mes)
    $nombre = "vuelos_{0}_{1:00}.zip" -f $anio, $mes
    $destino = Join-Path $carpeta $nombre

    if ((Test-Path $destino) -and ((Get-Item $destino).Length -gt $minimoBytes)) {
        Write-Host ("mes {0:00}: ya existe, se omite ({1})" -f $mes, $nombre)
        $yaEstaban = $yaEstaban + 1
        continue
    }

    Write-Host ("mes {0:00}: descargando {1} ..." -f $mes, $nombre)
    try {
        Invoke-WebRequest -Uri $url -OutFile $destino -UserAgent "Mozilla/5.0" -UseBasicParsing
        $tamanoMb = [math]::Round((Get-Item $destino).Length / 1MB, 1)
        if ((Get-Item $destino).Length -le $minimoBytes) {
            Write-Host ("mes {0:00}: ERROR, el archivo pesa solo {1} MB (se esperaban 25 a 35 MB)" -f $mes, $tamanoMb)
            Remove-Item $destino -Force
            $fallaron = $fallaron + $mes
        }
        else {
            Write-Host ("mes {0:00}: listo ({1} MB)" -f $mes, $tamanoMb)
            $descargados = $descargados + 1
        }
    }
    catch {
        Write-Host ("mes {0:00}: ERROR al descargar: {1}" -f $mes, $error[0].Exception.Message)
        if (Test-Path $destino) {
            Remove-Item $destino -Force
        }
        $fallaron = $fallaron + $mes
    }
}

Write-Host ""
Write-Host "Resumen"
Write-Host ("  Descargados ahora : {0}" -f $descargados)
Write-Host ("  Ya estaban        : {0}" -f $yaEstaban)
Write-Host ("  Con error         : {0}" -f $fallaron.Count)
if ($fallaron.Count -gt 0) {
    Write-Host ("  Meses con error   : {0}" -f ($fallaron -join ", "))
    Write-Host "  Vuelve a correr el script: solo intenta los meses que faltan."
    Write-Host "  Si sigue fallando, abre las ligas con -SoloListar en el navegador y descargalas a mano."
}
else {
    Write-Host ("  Carpeta           : {0}" -f $carpeta)
    Write-Host "  Siguiente paso: sube estos 12 archivos al volumen (ver GUIA.md, Paso 1)."
}
