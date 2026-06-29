# 生成 Go 代码：需要 protoc 与 protoc-gen-go
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root

$protoc = Get-Command protoc -ErrorAction SilentlyContinue
if (-not $protoc) {
    $bundled = Get-ChildItem -Recurse .tools\protoc -Filter protoc.exe -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($bundled) {
        $protoc = $bundled.FullName
    }
}
if (-not $protoc) {
    Write-Host "protoc not found, downloading portable build..."
    New-Item -ItemType Directory -Force -Path .tools\protoc | Out-Null
    Invoke-WebRequest -Uri "https://github.com/protocolbuffers/protobuf/releases/download/v29.3/protoc-29.3-win64.zip" -OutFile .tools\protoc.zip
    Expand-Archive -Path .tools\protoc.zip -DestinationPath .tools\protoc -Force
    $protoc = (Get-ChildItem -Recurse .tools\protoc -Filter protoc.exe | Select-Object -First 1).FullName
}

if (-not (Get-Command protoc-gen-go -ErrorAction SilentlyContinue)) {
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
}

& $protoc --go_out=api/pb --go_opt=paths=source_relative -I api/proto api/proto/messages.proto
Write-Host "generated api/pb/messages.pb.go"
