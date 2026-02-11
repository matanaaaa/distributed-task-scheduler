# make_zipbomb.ps1
# Generates task_zipbomb.zip that is tiny but expands to >128MB when extracted (zip bomb test)

$ErrorActionPreference = "Stop"

Remove-Item -Force .\task_zipbomb.zip -ErrorAction SilentlyContinue

Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem

$zipPath = Join-Path (Get-Location) "task_zipbomb.zip"
if (Test-Path $zipPath) { Remove-Item -Force $zipPath }

$zip = [System.IO.Compression.ZipFile]::Open($zipPath, [System.IO.Compression.ZipArchiveMode]::Create)
try {
  # Keep contract complete
  $run = $zip.CreateEntry("run.sh")
  $sw = New-Object System.IO.StreamWriter($run.Open(), (New-Object System.Text.UTF8Encoding($false)))
  $sw.Write("#!/bin/sh`nmkdir -p output`necho ok > output/ok.txt`n")
  $sw.Dispose()

  # total uncompressed: 20MB * 10 = 200MB (>128MB)
  $chunkSize = 20MB
  $chunks = 10

  # repetitive data -> compresses extremely well
  $buf = New-Object byte[] (1MB)
  for ($i=0; $i -lt $buf.Length; $i++) { $buf[$i] = 65 }  # 'A'

  for ($k=1; $k -le $chunks; $k++) {
    $entryName = ("data/chunk_{0}.bin" -f $k)
    $e = $zip.CreateEntry($entryName)
    $s = $e.Open()

    $toWrite = [int64]$chunkSize
    while ($toWrite -gt 0) {
      $n = [int]([Math]::Min($buf.Length, $toWrite))
      $s.Write($buf, 0, $n)
      $toWrite -= $n
    }
    $s.Dispose()
  }
}
finally {
  $zip.Dispose()
}

Write-Host "Generated: $zipPath"
Get-Item .\task_zipbomb.zip
