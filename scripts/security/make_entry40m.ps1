# make_entry40m.ps1
# Generates task_entry40m.zip where a single entry expands to 40MB (>32MB entry limit)

$ErrorActionPreference = "Stop"

Remove-Item -Force .\task_entry40m.zip -ErrorAction SilentlyContinue

Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem

$zipPath = Join-Path (Get-Location) "task_entry40m.zip"
if (Test-Path $zipPath) { Remove-Item -Force $zipPath }

$zip = [System.IO.Compression.ZipFile]::Open($zipPath, [System.IO.Compression.ZipArchiveMode]::Create)
try {
  # Keep contract complete
  $run = $zip.CreateEntry("run.sh")
  $sw = New-Object System.IO.StreamWriter($run.Open(), (New-Object System.Text.UTF8Encoding($false)))
  $sw.Write("#!/bin/sh`nmkdir -p output`necho ok > output/ok.txt`n")
  $sw.Dispose()

  # single file: 40MB
  $e = $zip.CreateEntry("big.bin")
  $s = $e.Open()

  $buf = New-Object byte[] (1MB)
  for ($i=0; $i -lt $buf.Length; $i++) { $buf[$i] = 66 }  # 'B'

  $toWrite = [int64](40MB)
  while ($toWrite -gt 0) {
    $n = [int]([Math]::Min($buf.Length, $toWrite))
    $s.Write($buf, 0, $n)
    $toWrite -= $n
  }
  $s.Dispose()
}
finally {
  $zip.Dispose()
}

Write-Host "Generated: $zipPath"
Get-Item .\task_entry40m.zip
