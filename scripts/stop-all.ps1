# 停止 NewGame 各服务（按监听端口）
$ErrorActionPreference = "SilentlyContinue"

$ports = @(
    8080,  # login
    9000,  # gate tcp
    9001,  # gate http
    9010,  # gate z2 tcp
    9011,  # gate z2 http
    9100,  # game
    9110,  # game z2
    9200,  # match
    9300,  # battle
    9400,  # social
    9500,  # mail
    9600,  # rank
    9700,  # activity
    9800,  # pay
    9900   # gm
)

$stopped = 0
foreach ($port in $ports) {
    $conns = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    foreach ($c in $conns) {
        $pid = $c.OwningProcess
        if ($pid -and $pid -ne 0) {
            Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
            Write-Host "stopped PID $pid (port $port)"
            $stopped++
        }
    }
}

if ($stopped -eq 0) {
    Write-Host "no listening processes found on game ports"
} else {
    Write-Host "stopped $stopped process(es)"
}
