<#
.SYNOPSIS
    Protocolo de execucao do experimento (secao 3.4 do artigo).

.DESCRIPTION
    Implementa os itens a) a f) do protocolo:
      a) reinicia integralmente o ambiente antes de cada cenario, com as tabelas
         do banco esvaziadas pelo descarte do volume;
      b) executa o cenario de aquecimento e descarta seus dados;
      c) repete cada combinacao de cenario, via e tamanho o numero de vezes
         solicitado;
      d) intercala as vias e os tamanhos, em ordem sorteada com semente fixa;
      e) cabe ao operador garantir a ausencia de outras cargas no hospedeiro;
      f) registra data, horario, versoes das imagens e configuracao de recursos.

    Conduz tambem o cenario C7, em que o conteiner do consumidor e interrompido
    durante a carga e reiniciado em seguida.

.EXAMPLE
    # Execucao final do desenho principal: C1 a C5, duas vias, dois tamanhos
    .\scripts\executar.ps1

.EXAMPLE
    # Cenarios complementares, apenas com 500 bytes
    .\scripts\executar.ps1 -Cenarios C6,C7 -Tamanhos 500

.EXAMPLE
    # Teste-piloto de calibracao: rampa em cada via e em cada tamanho
    .\scripts\executar.ps1 -Cenarios C5 -Repeticoes 1 -SemAquecimento -Rotulo piloto

.EXAMPLE
    # Verificacao rapida do prototipo (nao tem valor experimental)
    .\scripts\executar.ps1 -Cenarios C2 -Repeticoes 1 -Duracao 60s -SemAquecimento -Rotulo verificacao
#>
[CmdletBinding()]
param(
    [string[]]$Cenarios = @('C1', 'C2', 'C3', 'C4', 'C5'),
    [string[]]$Modos = @('sincrono', 'assincrono'),
    [int[]]$Tamanhos = @(500, 5000),
    [int]$Repeticoes = 3,
    # Primeira repeticao desta sessao. Permite dividir a execucao final em dias
    # sem que uma sessao apague as pastas da anterior.
    [int]$RepeticaoInicial = 1,
    [int]$DrenoMaximoS = 600,
    [string]$Duracao = '',
    [int]$Carga = 0,
    [string]$AquecimentoDuracao = '',
    [switch]$SemAquecimento,
    [switch]$SemConsolidar,
    [string]$Rotulo = '',
    [string]$Python = 'python',
    [double]$IntervaloAmostra = 1.0,
    [int]$Semente = 20261025,
    # C7: instante da interrupcao do consumidor e sua duracao (Quadro 6).
    [int]$QuedaAosS = 120,
    [int]$QuedaDuracaoS = 30
)

# Mantido em Continue porque o docker escreve o progresso em stderr, o que em
# Windows PowerShell 5.1 interromperia o script. As falhas relevantes sao
# verificadas por codigo de saida.
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

# Taxa de chegada por cenario, em pedidos por segundo (Quadro 6). A carga e
# definida por taxa, e nao por usuarios virtuais concorrentes, para que as duas
# vias recebam a mesma demanda.
#
# O teto e de 250, e nao 400: o piloto mostrou que acima de 248 pedidos/s a via
# sincrona com 500 bytes leva o produtor ao limite de CPU e o gerador deixa de
# emitir toda a demanda, de modo que a medicao passaria a refletir o
# dimensionamento do equipamento, e nao o estilo de integracao (secao 3.6).
$CARGA_CENARIO = @{ C0 = 25; C1 = 50; C2 = 100; C3 = 200; C4 = 250; C5 = 250; C6 = 250; C7 = 100 }

function Agora-Ms {
    return [long]([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())
}

function Passo($texto) {
    # Marcacao de etapa, no console e em resultados/<execucao>/progresso.log.
    # Numa sessao de horas e o que permite saber onde a execucao esta.
    $linha = "{0:HH:mm:ss}  {1}" -f (Get-Date), $texto
    Write-Host ("  " + $linha) -ForegroundColor DarkGray
    if ($script:caminhoProgresso) {
        Add-Content -Path $script:caminhoProgresso -Value $linha -Encoding UTF8
    }
}

function Carregar-Env {
    $mapa = @{}
    $caminho = Join-Path $raiz '.env'
    if (Test-Path $caminho) {
        foreach ($linha in Get-Content $caminho) {
            $texto = $linha.Trim()
            if ($texto -eq '' -or $texto.StartsWith('#')) { continue }
            $pos = $texto.IndexOf('=')
            if ($pos -lt 1) { continue }
            $mapa[$texto.Substring(0, $pos).Trim()] = $texto.Substring($pos + 1).Trim()
        }
    }
    # Variaveis definidas no processo tem precedencia sobre o .env, como no
    # proprio Compose, de modo que a calibracao possa sobrescrever um parametro
    # sem editar o arquivo. O valor efetivo e o que vai aos metadados.
    foreach ($chave in @($mapa.Keys)) {
        $doProcesso = [System.Environment]::GetEnvironmentVariable($chave, 'Process')
        if ($null -ne $doProcesso -and $doProcesso -ne '') { $mapa[$chave] = $doProcesso }
    }
    return $mapa
}

function Valor-Env($mapa, $chave, $padrao) {
    if ($mapa.ContainsKey($chave) -and $mapa[$chave] -ne '') { return $mapa[$chave] }
    return $padrao
}

function Credencial-RabbitMQ($mapa) {
    $usuario = Valor-Env $mapa 'RABBITMQ_USUARIO' 'exp'
    $senha = Valor-Env $mapa 'RABBITMQ_SENHA' 'exp'
    $par = "{0}:{1}" -f $usuario, $senha
    $bytes = [System.Text.Encoding]::ASCII.GetBytes($par)
    return @{ Authorization = 'Basic ' + [Convert]::ToBase64String($bytes) }
}

function Mensagens-Pendentes($cabecalhos) {
    # Consulta todas as filas e filtra pelo nome, para nao depender da
    # codificacao do nome do host virtual na URL.
    try {
        $filas = Invoke-RestMethod -Uri 'http://localhost:15672/api/queues' -Headers $cabecalhos -TimeoutSec 5
    }
    catch {
        return -1
    }
    foreach ($fila in $filas) {
        if ($fila.name -eq 'pedidos.novos') {
            $total = 0
            if ($fila.messages) { $total = [int]$fila.messages }
            return $total
        }
    }
    return 0
}

function Escrever-Json($caminho, $objeto) {
    $json = $objeto | ConvertTo-Json -Depth 8
    $codificacao = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($caminho, $json, $codificacao)
}

function Ler-Info($url) {
    try {
        return Invoke-RestMethod -Uri $url -TimeoutSec 10
    }
    catch {
        return $null
    }
}

function Versao-Imagem($servico) {
    try {
        $id = (docker compose images -q $servico 2>$null | Select-Object -First 1)
        if (-not $id) { return '' }
        return (docker image inspect $id --format '{{index .RepoTags 0}} {{.Id}}' 2>$null)
    }
    catch {
        return ''
    }
}

function Argumentos-K6($modo, $cenario, $execucaoId, $duracao, $carga, $tamanho, $mapaEnv) {
    $argumentos = @(
        'run', '--rm',
        '--name', 'exp-integracao-carga-k6',
        '--network', 'exp-integracao_default',
        '--cpus', (Valor-Env $mapaEnv 'CPU_CARGA' '2.0'),
        '--memory', (Valor-Env $mapaEnv 'MEM_CARGA' '512m'),
        # Executa como root apenas para gravar o resumo no diretorio montado.
        '--user', 'root',
        '-v', ((Join-Path $raiz 'carga\k6') + ':/scripts:ro'),
        '-v', ((Join-Path $raiz 'resultados') + ':/resultados'),
        '-e', ('MODO=' + $modo),
        '-e', ('CENARIO=' + $cenario),
        '-e', ('EXECUCAO_ID=' + $execucaoId),
        '-e', 'BASE_URL=http://produtor:8000',
        '-e', ('TEMPO_LIMITE_MS=' + (Valor-Env $mapaEnv 'TEMPO_LIMITE_MS' '1000')),
        '-e', ('TAMANHO_MENSAGEM=' + $tamanho),
        '-e', ('SEMENTE=' + (Get-Random -Minimum 1 -Maximum 2147483646)),
        # Cauda de C6: tempo a 50 pedidos/s depois do pico, dimensionado para
        # que a fila esvazie dentro da execucao.
        '-e', ('C6_CAUDA=' + (Valor-Env $mapaEnv 'C6_CAUDA' '4m'))
    )
    if ($duracao -ne '') { $argumentos += @('-e', ('DURACAO=' + $duracao)) }
    if ($carga -gt 0) { $argumentos += @('-e', ('CARGA=' + $carga)) }
    $argumentos += @((Valor-Env $mapaEnv 'IMAGEM_K6' 'grafana/k6:2.2.0'), 'run', '/scripts/pedido.js')
    return $argumentos
}

function Citar-Argumentos($argumentos) {
    # Start-Process une os argumentos por espaco sem citar nenhum deles: os
    # caminhos de montagem precisam vir entre aspas, ou o docker recebe
    # argumentos partidos e encerra sem executar o roteiro.
    $citados = $argumentos | ForEach-Object {
        if ($_ -match '\s') { '"' + $_ + '"' } else { $_ }
    }
    return ($citados -join ' ')
}

function Limpar-ConteinerK6 {
    # O nome do conteiner do gerador e fixo, para que a amostragem de recursos o
    # identifique. Uma execucao interrompida o deixaria para tras e faria a
    # seguinte falhar por conflito de nome.
    [void](docker rm -f exp-integracao-carga-k6 2>&1)
}

function Executar-K6($modo, $cenario, $execucaoId, $duracao, $carga, $tamanho, $mapaEnv) {
    $argumentos = Argumentos-K6 $modo $cenario $execucaoId $duracao $carga $tamanho $mapaEnv
    Limpar-ConteinerK6
    Passo ("gerador de carga iniciado: " + $cenario)
    Write-Host ('    docker ' + ($argumentos -join ' ')) -ForegroundColor DarkGray
    & docker @argumentos
    if ($LASTEXITCODE -ne 0) { Write-Warning ('k6 encerrou com codigo ' + $LASTEXITCODE) }
    Passo ("gerador de carga encerrado: " + $cenario)
}

function Executar-C7($modo, $execucaoId, $duracao, $carga, $tamanho, $mapaEnv, $metadados) {
    <#
        Queda temporaria do consumidor (Quadro 6). O gerador roda em segundo
        plano enquanto o script interrompe e reergue o conteiner, de modo que os
        dois instantes caiam dentro da janela de carga.
    #>
    $argumentos = Argumentos-K6 $modo 'C7' $execucaoId $duracao $carga $tamanho $mapaEnv
    Limpar-ConteinerK6
    Write-Host ('    docker ' + ($argumentos -join ' ')) -ForegroundColor DarkGray
    $k6 = Start-Process -FilePath 'docker' -ArgumentList (Citar-Argumentos $argumentos) `
        -PassThru -NoNewWindow

    Start-Sleep -Seconds $QuedaAosS
    Write-Host ("    interrompendo o consumidor aos {0} s" -f $QuedaAosS) -ForegroundColor DarkGray
    $metadados['queda_inicio_ms'] = Agora-Ms
    [void](docker compose stop consumidor 2>&1)

    Start-Sleep -Seconds $QuedaDuracaoS
    Write-Host ("    reerguendo o consumidor apos {0} s" -f $QuedaDuracaoS) -ForegroundColor DarkGray
    [void](docker compose start consumidor 2>&1)
    # O instante do retorno e aquele em que o consumidor volta a responder, e nao
    # o em que o docker compose start termina: e dele que parte o tempo de
    # recuperacao medido por coleta/eventos.py.
    $limiteVolta = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $limiteVolta) {
        try {
            Invoke-RestMethod -Uri 'http://localhost:8001/saude' -TimeoutSec 2 | Out-Null
            break
        }
        catch {
            Start-Sleep -Milliseconds 250
        }
    }
    $metadados['queda_fim_ms'] = Agora-Ms
    $metadados['queda_retorno_s'] = [math]::Round(
        ((Agora-Ms) - $metadados['queda_inicio_ms']) / 1000.0, 2)

    $k6.WaitForExit()
    # Refresh e necessario porque o objeto devolvido por Start-Process nao
    # atualiza ExitCode sozinho, o que produziria um aviso falso.
    $k6.Refresh()
    $codigo = $k6.ExitCode
    if ($null -ne $codigo -and $codigo -ne 0) {
        Write-Warning ('k6 encerrou com codigo ' + $codigo + ': a execucao nao tem dados')
    }
}

function Invoke-Execucao($cenario, $modo, $tamanho, $repeticao, $ordem, $mapaEnv, $cabecalhos) {
    $sufixo = ''
    if ($Rotulo -ne '') { $sufixo = '_' + $Rotulo }
    $execucaoId = "{0}_{1}_{2}b_r{3}{4}" -f $cenario, $modo, $tamanho, $repeticao, $sufixo
    $pasta = Join-Path (Join-Path $raiz 'resultados') $execucaoId

    Write-Host ''
    Write-Host ("== {0} | via {1} | {2} bytes | repeticao {3} ==" -f `
            $cenario, $modo, $tamanho, $repeticao) -ForegroundColor Cyan

    if (Test-Path $pasta) { Remove-Item $pasta -Recurse -Force }
    New-Item -ItemType Directory -Path $pasta -Force | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $pasta 'aquecimento') -Force | Out-Null
    $script:caminhoProgresso = Join-Path $pasta 'progresso.log'

    # a) ambiente reiniciado integralmente, com as tabelas do banco esvaziadas
    #    pelo descarte do volume, para que filas, conexoes, caches e dados
    #    gravados partam de estado conhecido.
    $env:EXECUCAO_ID = $execucaoId
    $env:CENARIO = $cenario
    $env:MODO = $modo
    $env:REPETICAO = "$repeticao"
    $env:TAMANHO_MENSAGEM = "$tamanho"

    Passo 'reiniciando o ambiente'
    [void](docker compose down -v --remove-orphans 2>&1)
    Passo 'ambiente derrubado'
    $saida = docker compose up -d --wait 2>&1
    Passo ("ambiente no ar (codigo " + $LASTEXITCODE + ")")
    if ($LASTEXITCODE -ne 0) {
        # Um arranque instavel nao deve interromper a sequencia de execucoes: a
        # saida e mostrada e o ambiente e recriado uma vez antes de desistir.
        Write-Warning 'o ambiente nao subiu na primeira tentativa; repetindo'
        $saida | ForEach-Object { Write-Host ('    ' + $_) -ForegroundColor DarkGray }
        [void](docker compose down -v --remove-orphans 2>&1)
        Start-Sleep -Seconds 10
        $saida = docker compose up -d --wait 2>&1
        if ($LASTEXITCODE -ne 0) {
            $saida | ForEach-Object { Write-Host ('    ' + $_) -ForegroundColor DarkGray }
            throw 'falha ao subir o ambiente'
        }
    }

    # f) registro de data, horario, versoes e configuracao de recursos.
    Passo 'lendo o /info do consumidor'
    $infoConsumidor = Ler-Info 'http://localhost:8001/info'
    $metadados = [ordered]@{
        execucao_id        = $execucaoId
        cenario            = $cenario
        modo               = $modo
        tamanho_mensagem   = $tamanho
        repeticao          = $repeticao
        carga_alvo_ops     = $(if ($Carga -gt 0) { $Carga } else { $CARGA_CENARIO[$cenario] })
        duracao_solicitada = $Duracao
        ordem_no_par       = $ordem
        semente_ordem      = $Semente
        rotulo             = $Rotulo
        iniciado_em        = (Get-Date).ToString('o')
        hospedeiro         = @{
            sistema     = (Get-CimInstance Win32_OperatingSystem).Caption
            versao      = [string][System.Environment]::OSVersion.Version
            processador = (Get-CimInstance Win32_Processor | Select-Object -First 1).Name
            memoria_mb  = [math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1MB)
        }
        ferramentas        = @{
            docker            = (docker --version)
            docker_compose    = (docker compose version)
            imagem_k6         = (Valor-Env $mapaEnv 'IMAGEM_K6' 'grafana/k6:2.2.0')
            imagem_produtor   = (Versao-Imagem 'produtor')
            imagem_consumidor = (Versao-Imagem 'consumidor')
            imagem_rabbitmq   = (Versao-Imagem 'rabbitmq')
            imagem_postgres   = (Versao-Imagem 'postgres')
        }
        # A durabilidade do banco vem do /info do consumidor: um
        # "docker compose exec" aqui travaria o script quando ele roda em
        # segundo plano com a entrada padrao redirecionada.
        banco              = $infoConsumidor.banco
        # As linhas sao unidas de proposito: o ConvertTo-Json do Windows
        # PowerShell 5.1 trava indefinidamente ao receber uma string com quebras
        # de linha num contexto sem sessao interativa, e o conteudo deste arquivo
        # seria a unica string assim nos metadados.
        wslconfig          = $(
            $arquivoWsl = Join-Path $env:USERPROFILE '.wslconfig'
            if (Test-Path $arquivoWsl) { ((Get-Content $arquivoWsl) -join '; ') }
            else { 'ausente' })
        configuracao       = $mapaEnv
        info_produtor      = (Ler-Info 'http://localhost:8000/info')
        info_consumidor    = $infoConsumidor
    }
    Escrever-Json (Join-Path $pasta 'metadados.json') $metadados
    Passo 'metadados gravados'

    # b) cenario de aquecimento executado e descartado.
    if (-not $SemAquecimento) {
        Passo 'aquecimento (C0, dados descartados)'
        Executar-K6 $modo 'C0' ($execucaoId + '/aquecimento') $AquecimentoDuracao 0 $tamanho $mapaEnv
        Start-Sleep -Seconds 5
        # Apenas o consumidor: a serie do produtor e gravada por worker e o
        # aquecimento e descartado na analise, pela janela de medicao.
        try {
            Invoke-RestMethod -Method Post -Uri 'http://localhost:8001/reiniciar-registro' -TimeoutSec 20 | Out-Null
        }
        catch {
            Write-Warning 'nao foi possivel reiniciar o registro do consumidor'
        }
    }

    # Amostragem de recursos e de ocupacao da fila durante a janela de medicao.
    # Os caminhos vao entre aspas porque Start-Process une os argumentos por
    # espaco, e a pasta do projeto pode conter espacos.
    $argsAmostra = @(
        ('"' + (Join-Path $raiz 'coleta\amostrar.py') + '"'),
        '--execucao-id', $execucaoId,
        '--dir', ('"' + (Join-Path $raiz 'resultados') + '"'),
        '--intervalo', "$IntervaloAmostra",
        '--usuario', (Valor-Env $mapaEnv 'RABBITMQ_USUARIO' 'exp'),
        '--senha', (Valor-Env $mapaEnv 'RABBITMQ_SENHA' 'exp')
    )
    $amostrador = Start-Process -FilePath $Python -ArgumentList $argsAmostra -PassThru -WindowStyle Hidden
    Passo 'amostragem iniciada'

    $inicio = Agora-Ms
    if ($cenario -eq 'C7') {
        Executar-C7 $modo $execucaoId $Duracao $Carga $tamanho $mapaEnv $metadados
    }
    else {
        Executar-K6 $modo $cenario $execucaoId $Duracao $Carga $tamanho $mapaEnv
    }
    $fim = Agora-Ms
    Passo 'carga encerrada'

    # Espera a drenagem da fila: na via assincrona, parte das operacoes conclui
    # depois do encerramento da carga, e essa conclusao integra a medicao. O que
    # sobrar na fila ao fim do prazo e contado como nao concluido.
    $drenagemS = 0
    if ($modo -eq 'assincrono') {
        Write-Host '  aguardando a drenagem da fila' -ForegroundColor DarkGray
        $limite = (Get-Date).AddSeconds($DrenoMaximoS)
        while ((Get-Date) -lt $limite) {
            $pendentes = Mensagens-Pendentes $cabecalhos
            if ($pendentes -le 0) { break }
            Start-Sleep -Seconds 2
        }
        $drenagemS = [math]::Round(((Agora-Ms) - $fim) / 1000.0, 2)
        Write-Host ("  drenagem concluida em {0} s" -f $drenagemS) -ForegroundColor DarkGray
    }
    else {
        Start-Sleep -Seconds 5
    }

    if (-not $amostrador.HasExited) { Stop-Process -Id $amostrador.Id -Force }
    Passo 'amostragem encerrada'

    # Conferencia da vazao: linhas gravadas na tabela de pedidos (Quadro 5).
    # A leitura e repetida ate estabilizar: ao fim da carga o consumidor ainda
    # conclui operacoes em voo, e uma contagem tirada cedo demais fica abaixo
    # dos registros T1 do arquivo, que e lido depois, na consolidacao.
    $contagem = $null
    $anterior = -1
    $limiteContagem = (Get-Date).AddSeconds(120)
    while ((Get-Date) -lt $limiteContagem) {
        $contagem = Ler-Info ('http://localhost:8001/pedidos/contagem?execucao_id=' + $execucaoId)
        if ($null -eq $contagem) { break }
        if ($contagem.linhas -eq $anterior) { break }
        $anterior = $contagem.linhas
        Start-Sleep -Seconds 3
    }
    Passo ("contagem do banco estabilizada: " + $(if ($null -ne $contagem) { $contagem.linhas } else { 'indisponivel' }))
    $metricas = Ler-Info 'http://localhost:8001/metricas'
    $metadados['inicio_carga_ms'] = $inicio
    $metadados['fim_carga_ms'] = $fim
    $metadados['drenagem_s'] = $drenagemS
    $metadados['linhas_banco'] = $(if ($null -ne $contagem) { $contagem.linhas } else { $null })
    $metadados['encerrado_em'] = (Get-Date).ToString('o')
    $metadados['metricas_consumidor'] = $metricas
    Escrever-Json (Join-Path $pasta 'metadados.json') $metadados

    Write-Host ("  concluido: {0} | linhas no banco: {1}" -f $pasta, $metadados['linhas_banco']) `
        -ForegroundColor Green
}

# --- Execucao --------------------------------------------------------------
$mapaEnv = Carregar-Env
$cabecalhos = Credencial-RabbitMQ $mapaEnv

# d) ordem das vias e dos tamanhos sorteada com semente fixa, de modo que a
#    sequencia seja reproduzivel e nenhum efeito de ordem recaia sempre sobre a
#    mesma condicao.
$condicoes = @()
foreach ($modo in $Modos) {
    foreach ($tamanho in $Tamanhos) {
        $condicoes += @{ modo = $modo; tamanho = $tamanho }
    }
}

$total = $Repeticoes * $Cenarios.Count * $condicoes.Count
Write-Host 'Experimento: integracao sincrona versus assincrona entre microsservicos' -ForegroundColor White
Write-Host ("Cenarios: {0} | Vias: {1} | Tamanhos: {2} | Repeticoes: {3} | Execucoes: {4}" -f `
    ($Cenarios -join ','), ($Modos -join ','), ($Tamanhos -join ','), $Repeticoes, $total)
Write-Host ("Semente da ordem: {0}" -f $Semente)
Write-Host 'Item e) do protocolo: encerre as demais aplicacoes de uso interativo antes de prosseguir.' -ForegroundColor Yellow

$indice = 0
for ($r = $RepeticaoInicial; $r -lt $RepeticaoInicial + $Repeticoes; $r++) {
    foreach ($cenario in $Cenarios) {
        # Semente derivada da repeticao e do cenario: a ordem varia entre eles e
        # se repete identica em uma nova execucao do script.
        $semeadura = $Semente + $r * 100 + [Array]::IndexOf($Cenarios, $cenario)
        Get-Random -SetSeed $semeadura | Out-Null
        $ordem = $condicoes | Sort-Object { Get-Random }
        $rotuloOrdem = (($ordem | ForEach-Object { $_.modo + '/' + $_.tamanho }) -join ' ')

        foreach ($condicao in $ordem) {
            $indice = $indice + 1
            Write-Host ''
            Write-Host ("[{0}/{1}]" -f $indice, $total) -ForegroundColor White
            Invoke-Execucao $cenario $condicao.modo $condicao.tamanho $r $rotuloOrdem $mapaEnv $cabecalhos
        }
    }
}

Write-Host ''
Write-Host 'Encerrando o ambiente' -ForegroundColor DarkGray
[void](docker compose down -v --remove-orphans 2>&1)

if (-not $SemConsolidar) {
    Write-Host 'Consolidando os dados' -ForegroundColor DarkGray
    # O --rotulo so entra quando ha rotulo: na execucao final ele e vazio, e o
    # PowerShell descarta o argumento vazio, de modo que o argparse receberia a
    # opcao sem valor e encerraria com erro.
    $argsConsolidar = @((Join-Path $raiz 'coleta\consolidar.py'),
        '--dir', (Join-Path $raiz 'resultados'))
    if ($Rotulo -ne '') { $argsConsolidar += @('--rotulo', $Rotulo) }
    & $Python @argsConsolidar
}

Write-Host ''
Write-Host 'Concluido. Verifique resultados\consolidado.csv e resultados\resumo.csv' -ForegroundColor Green
Write-Host 'Graficos: python coleta\graficos.py' -ForegroundColor Green
Write-Host 'Cenarios C6 e C7: python coleta\eventos.py' -ForegroundColor Green
