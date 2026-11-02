<#
.SYNOPSIS
    Calibracao do intermediador de mensagens (apoio ao teste-piloto, secao 3.4).

.DESCRIPTION
    Responde a uma pergunta de validade interna: sob carga, o que limita a via
    assincrona e o estilo de integracao ou o limite de CPU imposto ao RabbitMQ?

    Submete a via assincrona a uma carga fixa, variando apenas o limite de CPU do
    intermediador e a persistencia da mensagem, e executa uma medicao de
    referencia da via sincrona sob a mesma carga. Se a vazao deixar de crescer
    quando o intermediador recebe mais CPU, o gargalo passou a ser um dos dois
    servicos, que e a condicao desejada: o fator manipulado volta a ser o estilo
    de integracao, e nao o dimensionamento da infraestrutura.

    Os limites dos dois servicos permanecem inalterados em todas as execucoes.

.EXAMPLE
    .\scripts\calibrar_broker.ps1

.EXAMPLE
    .\scripts\calibrar_broker.ps1 -Carga 200 -Duracao 90s -Cpus 1.0,2.0
#>
[CmdletBinding()]
param(
    [int]$Carga = 150,
    # A grade usa um unico tamanho: o alvo e o dimensionamento do broker.
    [string]$Duracao = '60s',
    [string[]]$Cpus = @('1.0', '2.0', '3.0'),
    [string[]]$Persistencias = @('1', '0'),
    [switch]$SemReferenciaSincrona,
    [string]$Prefixo = 'calib',
    [string]$Python = 'python'
)

$ErrorActionPreference = 'Continue'
$raiz = Split-Path -Parent $PSScriptRoot
Set-Location $raiz

$executar = Join-Path $PSScriptRoot 'executar.ps1'

function Rotulo-Combinacao($cpu, $persistente, $modo) {
    # Sem pontos nem sinais, porque o rotulo compoe o nome da pasta.
    $cpuTexto = ($cpu -replace '[^0-9]', '')
    return "{0}_{1}_cpu{2}_p{3}" -f $Prefixo, $modo, $cpuTexto, $persistente
}

$combinacoes = @()
foreach ($persistente in $Persistencias) {
    foreach ($cpu in $Cpus) {
        $combinacoes += @{ modo = 'assincrono'; cpu = $cpu; persistente = $persistente }
    }
}
if (-not $SemReferenciaSincrona) {
    # A via sincrona nao usa o intermediador: uma execucao basta como referencia.
    $combinacoes += @{ modo = 'sincrono'; cpu = $Cpus[0]; persistente = $Persistencias[0] }
}

Write-Host 'Calibracao do intermediador de mensagens' -ForegroundColor White
Write-Host ("Carga fixa: {0} operacoes por segundo por {1}" -f $Carga, $Duracao)
Write-Host ("Execucoes: {0}" -f $combinacoes.Count)
Write-Host 'Encerre as demais aplicacoes de uso interativo antes de prosseguir.' -ForegroundColor Yellow

$indice = 0
foreach ($combinacao in $combinacoes) {
    $indice = $indice + 1
    $rotulo = Rotulo-Combinacao $combinacao.cpu $combinacao.persistente $combinacao.modo
    Write-Host ''
    Write-Host ("[{0}/{1}] via {2} | CPU do broker {3} | persistencia {4}" -f `
            $indice, $combinacoes.Count, $combinacao.modo, $combinacao.cpu, $combinacao.persistente) `
        -ForegroundColor Cyan

    # Sobrescreve os parametros no processo: o Compose e o executar.ps1 dao
    # precedencia a eles sobre o .env, e o valor efetivo vai aos metadados.
    $env:CPU_BROKER = $combinacao.cpu
    $env:MENSAGEM_PERSISTENTE = $combinacao.persistente

    & $executar -Cenarios C2 -Repeticoes 1 -Modos $combinacao.modo `
        -Tamanhos 500 -Duracao $Duracao -Carga $Carga -SemAquecimento -SemConsolidar `
        -Rotulo $rotulo -Python $Python
}

Remove-Item Env:\CPU_BROKER -ErrorAction SilentlyContinue
Remove-Item Env:\MENSAGEM_PERSISTENTE -ErrorAction SilentlyContinue

Write-Host ''
Write-Host 'Reunindo a grade de calibracao' -ForegroundColor DarkGray
& $Python (Join-Path $raiz 'coleta\consolidar.py') --dir (Join-Path $raiz 'resultados') --rotulo-prefixo $Prefixo

Write-Host ''
Write-Host ("Tabela: resultados\consolidado_{0}.csv" -f $Prefixo) -ForegroundColor Green
Write-Host 'Leitura: compare vazao_janela e cpu_rabbitmq_pct ao longo de cpu_broker_limite.' -ForegroundColor Green
Write-Host 'Se a vazao parar de crescer e cpu_produtor_pct ou cpu_consumidor_pct chegar ao teto,' -ForegroundColor Green
Write-Host 'o gargalo deixou de ser o intermediador.' -ForegroundColor Green
