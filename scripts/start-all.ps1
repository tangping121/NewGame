# 在项目根目录下依次启动所有服务（每个服务新窗口）
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root

$services = @(
    @{ Name = "login";    Target = "run-login" },
    @{ Name = "game";     Target = "run-game" },
    @{ Name = "gate";     Target = "run-gate" },
    @{ Name = "match";    Target = "run-match" },
    @{ Name = "battle";   Target = "run-battle" },
    @{ Name = "social";   Target = "run-social" },
    @{ Name = "mail";     Target = "run-mail" },
    @{ Name = "rank";     Target = "run-rank" },
    @{ Name = "activity"; Target = "run-activity" },
    @{ Name = "pay";      Target = "run-pay" },
    @{ Name = "gm";       Target = "run-gm" }
)

foreach ($svc in $services) {
    Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$root'; make $($svc.Target)"
    Start-Sleep -Seconds 1
}

Write-Host "All services started in separate windows."
