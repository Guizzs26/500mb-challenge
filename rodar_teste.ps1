# Script simples de Teste de Carga com docker-stats

Write-Host "=> Subindo infraestrutura..." -ForegroundColor Green
docker compose up -d
Start-Sleep -Seconds 3

Write-Host "=> Iniciando captura de docker-stats..." -ForegroundColor Green

$statsJob = Start-Job -ScriptBlock {
    "TIMESTAMP,CONTAINER,CPU,MEMORY,NET_IO,BLOCK_IO" | Out-File -FilePath "relatorio_hardware.csv" -Encoding UTF8
    while ($true) {
        $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff"
        docker stats --no-stream --format "$timestamp,{{.Container}},{{.CPUPerc}},{{.MemUsage}},{{.NetIO}},{{.BlockIO}}" | Out-File -FilePath "relatorio_hardware.csv" -Append -Encoding UTF8
        Start-Sleep -Milliseconds 500
    }
}

Start-Sleep -Seconds 1

Write-Host "=> Iniciando teste k6 (SCENARIO=mixed, VUS=30, BATCH_SIZE=100)..." -ForegroundColor Green

Get-Content script.js | docker run --rm -i --network 500mb-challenge_default `
  -e SCENARIO=mixed -e BASE_URL=http://nginx:8080 -e VUS=30 -e BATCH_SIZE=100 `
  loadimpact/k6 run - | Tee-Object -FilePath "resultado_k6.txt"

Start-Sleep -Seconds 2
Stop-Job -Job $statsJob -ErrorAction SilentlyContinue
Remove-Job -Job $statsJob -ErrorAction SilentlyContinue

Write-Host "Teste concluido! Relatorios salvos:" -ForegroundColor Green
Write-Host "  - relatorio_hardware.csv (docker-stats durante o teste)"
Write-Host "  - resultado_k6.txt (resultados do k6)"
