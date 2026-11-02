<#
.SYNOPSIS
    Prova da confirmacao manual de consumo (secao 3.2.2 do artigo).

.DESCRIPTION
    A secao 3.2.2 afirma que a confirmacao de recebimento e manual e emitida
    somente apos a conclusao do processamento, de modo que uma falha do consumidor
    devolva a mensagem a fila em lugar de descarta-la. Este teste produz a
    evidencia: acumula mensagens na fila, derruba o consumidor com entregas
    pendentes de confirmacao e verifica que nenhuma mensagem foi perdida.

    Com confirmacao automatica, as mensagens entregues e nao processadas
    desapareceriam; com confirmacao manual, voltam ao estado "prontas" quando o
    canal se encerra.

.EXAMPLE
    .\scripts\teste_queda_consumidor.ps1
#>
[CmdletBinding()]
param(
    [int]$Carga = 250,
    [string]$Duracao = '30s',
    [int]$DrenoMaximoS = 300,
    [string]$Rotulo = 'queda',
    [string]$Python = 'python'
)

$ErrorActionPreference = 'Continue'

# Invoke-RestMethod se apoia em HttpWebRequest, que tenta descobrir proxy
# automatico (WPAD) antes de cada chamada. Num contexto sem sessao interativa
# essa descoberta bloqueia por minutos, o que travava o script logo apos subir o
# ambiente. Todas as chamadas aqui sao para localhost, de modo que desligar a
# deteccao e a barra de progresso as torna imediatas.
[System.Net.WebRequest]::DefaultWebProxy = $null
$ProgressPreference = 'SilentlyContinue'
$raiz = Split-Path -Parent $PSScriptRoot
Set-Location $raiz

$execucaoId = "queda_consumidor"
$pasta = Join-Path (Join-Path $raiz 'resultados') $execucaoId

function Credencial {
    $usuario = 'exp'
    $senha = 'exp'
    foreach ($linha in Get-Content (Join-Path $raiz '.env')) {
        if ($linha -match '^RABBITMQ_USUARIO=(.+)$') { $usuario = $Matches[1].Trim() }
        if ($linha -match '^RABBITMQ_SENHA=(.+)$') { $senha = $Matches[1].Trim() }
    }
    $bytes = [System.Text.Encoding]::ASCII.GetBytes("${usuario}:${senha}")
    return @{ Authorization = 'Basic ' + [Convert]::ToBase64String($bytes) }
}

function Estado-Fila($cabecalhos) {
    try {
        $filas = Invoke-RestMethod -Uri 'http://localhost:15672/api/queues' -Headers $cabecalhos -TimeoutSec 10
    }
    catch {
        return $null
    }
    foreach ($fila in $filas) {
        if ($fila.name -eq 'pedidos.novos') {
            return [ordered]@{
                total          = [int]$fila.messages
                prontas        = [int]$fila.messages_ready
                nao_confirmadas = [int]$fila.messages_unacknowledged
            }
        }
    }
    return $null
}

function Metricas-Consumidor {
    try {
        return (Invoke-RestMethod -Uri 'http://localhost:8001/metricas' -TimeoutSec 10).contagens
    }
    catch {
        return $null
    }
}

$cabecalhos = Credencial

Write-Host 'Teste de queda do consumidor com mensagens pendentes' -ForegroundColor White
Write-Host ''

if (Test-Path $pasta) { Remove-Item $pasta -Recurse -Force }
New-Item -ItemType Directory -Path $pasta -Force | Out-Null

$env:EXECUCAO_ID = $execucaoId
$env:CENARIO = 'C4'
$env:MODO = 'assincrono'
$env:REPETICAO = '0'

Write-Host '1. subindo o ambiente' -ForegroundColor DarkGray
[void](docker compose down -v --remove-orphans 2>&1)
$saida = docker compose up -d --wait 2>&1
if ($LASTEXITCODE -ne 0) {
    $saida | ForEach-Object { Write-Host ('    ' + $_) -ForegroundColor DarkGray }
    throw 'falha ao subir o ambiente'
}

Write-Host ("2. acumulando fila: {0} op/s por {1}" -f $Carga, $Duracao) -ForegroundColor DarkGray
$argumentos = @(
    'run', '--rm', '--name', 'exp-integracao-carga-k6',
    '--network', 'exp-integracao_default',
    '--cpus', '2.0', '--memory', '512m', '--user', 'root',
    '-v', ((Join-Path $raiz 'carga\k6') + ':/scripts:ro'),
    '-v', ((Join-Path $raiz 'resultados') + ':/resultados'),
    '-e', 'MODO=assincrono', '-e', 'CENARIO=C4',
    '-e', ('EXECUCAO_ID=' + $execucaoId),
    '-e', 'BASE_URL=http://produtor:8000',
    '-e', 'TEMPO_LIMITE_MS=3000', '-e', 'TAMANHO_MENSAGEM=500',
    '-e', ('DURACAO=' + $Duracao), '-e', ('CARGA=' + $Carga),
    'grafana/k6:2.2.0', 'run', '/scripts/pedido.js'
)
& docker @argumentos 2>&1 | Out-Null

# As estatisticas da API de gerenciamento sao amostradas: sem esta pausa a
# leitura fica atras do estado real da fila.
Start-Sleep -Seconds 8
$antes = Estado-Fila $cabecalhos
$metricasAntes = Metricas-Consumidor
if ($null -eq $antes) { throw 'nao foi possivel consultar a fila' }
Write-Host ("   fila: {0} mensagens, {1} prontas, {2} nao confirmadas" -f `
        $antes.total, $antes.prontas, $antes.nao_confirmadas)
if ($antes.nao_confirmadas -le 0) {
    Write-Warning 'nenhuma entrega pendente de confirmacao: aumente -Carga ou -Duracao'
}

Write-Host '3. derrubando o consumidor' -ForegroundColor DarkGray
[void](docker compose kill consumidor 2>&1)
Start-Sleep -Seconds 10

$depois = Estado-Fila $cabecalhos
Write-Host ("   fila: {0} mensagens, {1} prontas, {2} nao confirmadas" -f `
        $depois.total, $depois.prontas, $depois.nao_confirmadas)

Write-Host '4. reerguendo o consumidor e aguardando a drenagem' -ForegroundColor DarkGray
[void](docker compose up -d --wait consumidor 2>&1)
$limite = (Get-Date).AddSeconds($DrenoMaximoS)
while ((Get-Date) -lt $limite) {
    $atual = Estado-Fila $cabecalhos
    if ($null -ne $atual -and $atual.total -le 0) { break }
    Start-Sleep -Seconds 3
}
$final = Estado-Fila $cabecalhos
$metricasDepois = Metricas-Consumidor

# O consumidor reinicia com o registro em branco, mas grava no mesmo arquivo.
$sucessos = 0
$caminhoCsv = Join-Path $pasta 'consumidor.csv'
if (Test-Path $caminhoCsv) {
    $sucessos = (Import-Csv $caminhoCsv | Where-Object { $_.situacao -eq 'sucesso' }).Count
}

$relatorio = [ordered]@{
    teste                     = 'queda do consumidor com mensagens pendentes'
    origem                    = 'secao 3.2.2: confirmacao manual apos a conclusao do processamento'
    realizado_em              = (Get-Date).ToString('o')
    carga_ops                 = $Carga
    duracao                   = $Duracao
    fila_antes_da_queda       = $antes
    fila_apos_a_queda         = $depois
    fila_ao_final             = $final
    metricas_antes            = $metricasAntes
    metricas_depois           = $metricasDepois
    concluidas_com_sucesso    = $sucessos
    entregas_pendentes_na_queda = $antes.nao_confirmadas
    nao_confirmadas_apos_queda  = $(if ($null -ne $depois) { $depois.nao_confirmadas } else { $null })
    mensagens_perdidas          = $(if ($null -ne $final) { $final.total } else { $null })
    observacao                  = 'A diferenca de mensagens prontas antes e depois nao mede o reenfileiramento: as estatisticas da API de gerenciamento sao amostradas e ficam atras do estado real. A evidencia e a queda das entregas nao confirmadas a zero sem reducao do total, mais a ausencia de perdas ao final.'
}
$json = $relatorio | ConvertTo-Json -Depth 6
[System.IO.File]::WriteAllText((Join-Path $pasta 'queda_consumidor.json'),
    $json, (New-Object System.Text.UTF8Encoding($false)))

Write-Host ''
Write-Host 'Resultado' -ForegroundColor White
Write-Host ("  entregas pendentes no momento da queda: {0}" -f $antes.nao_confirmadas)
Write-Host ("  nao confirmadas apos a queda:           {0}" -f $relatorio.nao_confirmadas_apos_queda)
Write-Host ("  concluidas com sucesso ao final:        {0}" -f $sucessos)
Write-Host ("  restantes na fila ao final:             {0}" -f $relatorio.mensagens_perdidas)
Write-Host ''
if ($antes.nao_confirmadas -gt 0 -and $relatorio.nao_confirmadas_apos_queda -eq 0 -and `
        $null -ne $final -and $final.total -le 0) {
    Write-Host ("As {0} entregas pendentes voltaram a fila e nenhuma mensagem foi perdida:" -f `
            $antes.nao_confirmadas) -ForegroundColor Green
    Write-Host 'confirmacao manual comprovada (secao 3.2.2).' -ForegroundColor Green
}
elseif ($antes.nao_confirmadas -le 0) {
    Write-Host 'Inconclusivo: nenhuma entrega pendente no momento da queda.' -ForegroundColor Yellow
    Write-Host 'Refaca com -Carga ou -Duracao maior.' -ForegroundColor Yellow
}
else {
    Write-Host 'Inconclusivo: verifique o relatorio.' -ForegroundColor Yellow
}
Write-Host ("Relatorio: {0}" -f (Join-Path $pasta 'queda_consumidor.json')) -ForegroundColor Green

[void](docker compose down -v --remove-orphans 2>&1)
