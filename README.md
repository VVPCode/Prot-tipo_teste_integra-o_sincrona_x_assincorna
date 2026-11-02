# Protótipo do experimento — integração síncrona versus assíncrona entre microsserviços

Objeto de estudo construído para o experimento controlado descrito na seção 3 de
`Entrega3_Hello_World_revisado_v3.docx`. São dois microsserviços que expõem a
**mesma** operação de negócio pelas duas vias de comunicação, com validação,
tempo artificial fixo de 50 ms e gravação transacional em PostgreSQL, sob os
mesmos limites de recursos. Um único fator é manipulado por vez: o estilo de
integração, o nível de carga e o tamanho da mensagem.

## Arquitetura

Cinco componentes, todos em contêiner com limites fixos de CPU e memória:

```
k6 (gerador, processo apartado)
      │  POST /pedidos
      ▼
  produtor ──── MODO=sincrono ────► consumidor ──► PostgreSQL
      │                                 ▲
      └──── MODO=assincrono ──► RabbitMQ ┘
```

O estilo é decidido pelo **produtor**, por parâmetro de configuração, e não pelo
endereço que o gerador escolhe. O PostgreSQL é exclusivo do consumidor.

T0 é registrado pelo produtor ao receber a requisição; T1, pelo consumidor
**depois do COMMIT**. Os dois são pareados pelo identificador de correlação, que
vai no cabeçalho HTTP na via síncrona e na propriedade `correlation_id` na
assíncrona.

## Mapeamento entre o artigo e o código

| Item do artigo | Onde está implementado |
| --- | --- |
| 3.2 PostgreSQL exclusivo do consumidor | serviço `postgres` no Compose, [db/schema.sql](db/schema.sql) |
| 3.2.1 Regra: validação, 50 ms e gravação em uma transação | [comum/negocio.py](comum/negocio.py), [comum/banco.py](comum/banco.py) |
| 3.2.1 Idempotência pela chave primária `pedido_id` | `INSERT ... ON CONFLICT DO NOTHING` em `comum/banco.py` |
| Quadro 3 Pedido com 12 campos e regras de validação | [comum/modelos.py](comum/modelos.py), [carga/k6/payload.js](carga/k6/payload.js) |
| Quadro 5 Tamanho da mensagem em dois níveis | `TAMANHO_MENSAGEM`; produtor registra o tamanho recebido |
| Quadro 5 Estilo por parâmetro do produtor | `MODO` em [comum/config.py](comum/config.py); um único `POST /pedidos` |
| 3.2.2 Via síncrona bloqueante, mesmo tempo limite, erro no esgotamento | `_via_sincrona` em [services/produtor/app.py](services/produtor/app.py) |
| 3.2.2 Via assíncrona com troca direta e fila durável | [comum/topologia.py](comum/topologia.py) |
| 3.2.2 Confirmação manual só depois do COMMIT | `mensagem.ack()` em [services/consumidor/app.py](services/consumidor/app.py) |
| 3.2.2 Pool de conexões fixo, igual nas duas vias | `POOL_BANCO` em `comum/banco.py` |
| 3.2.2 Limite de entregas igual ao semáforo | `CONCORRENCIA`: semáforo e `prefetch_count` |
| Quadro 5 T0 no produtor, T1 após o COMMIT | [comum/registro.py](comum/registro.py) |
| Quadro 5 Vazão conferida pela contagem de linhas | `GET /pedidos/contagem`, gravado em `metadados.json` |
| Quadro 5 Taxa de erro, inclusive falha de gravação | `falha_gravacao` em `comum/negocio.py`, [coleta/consolidar.py](coleta/consolidar.py) |
| Quadro 5 Recursos a cada 1 s, média e pico | [coleta/amostrar.py](coleta/amostrar.py), `coleta/consolidar.py` |
| Quadro 6 Cenários C0 a C7 | [carga/k6/pedido.js](carga/k6/pedido.js), C7 conduzido por `scripts/executar.ps1` |
| 3.4 Protocolo, itens a) a f) | [scripts/executar.ps1](scripts/executar.ps1) |
| 3.4 Ordem sorteada com semente fixa | `-Semente` em `scripts/executar.ps1` |
| 3.5 Análise por tamanho e ponto de inversão | [coleta/rampa.py](coleta/rampa.py), [coleta/graficos.py](coleta/graficos.py) |
| 3.5 Indicadores de C6 e C7 | [coleta/eventos.py](coleta/eventos.py) |
| 3.2.2 Prova da confirmação manual | [scripts/teste_queda_consumidor.ps1](scripts/teste_queda_consumidor.ps1) |

## Estrutura

```
prototipo/
  db/schema.sql          tabelas de pedidos e de itens
  comum/                 configuração, contrato, regra de negócio, banco, topologia, registro
  services/produtor/     expõe POST /pedidos; decide a via por MODO; registra T0
  services/consumidor/   valida, espera 50 ms, grava; T1 e ack depois do COMMIT
  carga/k6/              gerador: cenários do Quadro 6 e mensagem do Quadro 3
  coleta/                amostragem, consolidação, rampa, eventos, gráficos
  scripts/               protocolo de execução, calibração do broker, teste de queda
  resultados/            dados brutos por execução e planilhas consolidadas
  docker-compose.yml     cinco serviços com limites fixos
  .env                   parâmetros controlados
```

## Pré-requisitos

- Docker Desktop em execução, com o back-end WSL 2.
- Python 3 no hospedeiro. A consolidação não tem dependências externas; os
  gráficos requerem `pip install -r coleta/requirements.txt`.
- O k6 é executado pela imagem `grafana/k6:2.2.0`, sem instalação no hospedeiro.

## Verificação rápida

```bash
docker compose build
```

```powershell
.\scripts\executar.ps1 -Cenarios C2 -Repeticoes 1 -Duracao 45s -SemAquecimento -Rotulo verificacao
```

```bash
python coleta/consolidar.py --rotulo verificacao
```

O critério de aceitação é o Portão 1 do planejamento: as duas vias e os dois
tamanhos recebem a mesma demanda, as linhas do banco igualam os registros T1 de
sucesso (`divergencia_banco` igual a zero), não há duplicatas e o gerador não
descarta demanda. A última verificação, de 45 s por célula, deu:

| | sínc./500 | sínc./5000 | assínc./500 | assínc./5000 |
| --- | --- | --- | --- | --- |
| Emitidos | 4.501 | 4.501 | 4.501 | 4.501 |
| Concluídos (T1) | 4.501 | 4.501 | 4.501 | 4.501 |
| Linhas no banco | 4.501 | 4.501 | 4.501 | 4.501 |
| Descartados pelo gerador | 0 | 0 | 0 | 0 |
| Tamanho do corpo (bytes) | 500 | 5.000 | 500 | 5.000 |
| Latência média (ms) | 60,1 | 56,5 | 54,2 | 54,6 |
| CPU do PostgreSQL, média/pico | 14 / 35 % | 10 / 30 % | 10 / 17 % | 10 / 17 % |

A latência de ~55 ms confirma a previsão do planejamento: 50 ms artificiais mais
alguns milissegundos de validação e gravação, o que põe a capacidade teórica em
torno de 145 pedidos/s com concorrência 8.

## Parâmetros fixados

| Parâmetro | Valor | Origem |
| --- | --- | --- |
| `TEMPO_PROCESSAMENTO_MS` | 50 | seção 3.2.1 |
| `CONCORRENCIA` | 8 | capacidade de ~145 pedidos/s, entre C2 e C3 |
| `POOL_BANCO` | 8 | igual à concorrência: menor seria gargalo escondido |
| `TEMPO_LIMITE_MS` | 1000 | cerca de 18 vezes o tempo de uma operação |
| `DrenoMaximoS` | 600 | C4 assíncrono acumula ~76 mil mensagens |
| `WORKERS_PRODUTOR` / `CPU_PRODUTOR` | 4 / 4.0 | um processo não passa de um núcleo |
| `CPU_SERVICO` (consumidor) | 1.0 | — |
| `CPU_BROKER` | 2.0 | grade de `scripts/calibrar_broker.ps1` |
| `CPU_POSTGRES` / `MEM_POSTGRES` | 1.0 / 1g | banco não pode ser o componente saturado |
| `CPU_CARGA` | 2.0 | mantém o gerador abaixo de 80 % do limite |
| `C6_CAUDA` | 4m | tempo a 50 pedidos/s após o pico, para a fila esvaziar |

Os limites somam 10 CPUs nas 12 threads do notebook do Quadro 4. A concorrência
de 8 operações simultâneas decorre da aritmética, não de preferência: fixado o
tempo de processamento em 50 ms, é o valor que põe a saturação entre C2 e C3.

## Teste-piloto (seção 3.4): verificado

```powershell
.\scripts\executar.ps1 -Cenarios C5 -Repeticoes 1 -SemAquecimento -Rotulo piloto
.\scripts\executar.ps1 -Cenarios C6,C7 -Tamanhos 500 -Repeticoes 1 -Rotulo piloto2
```

```bash
python coleta/consolidar.py --rotulo piloto && python coleta/rampa.py --rotulo piloto
python coleta/consolidar.py --rotulo piloto2 && python coleta/eventos.py
```

Oito execuções em 79 min: C5 nas quatro condições, mais C6 e C7 nas duas vias
com 500 bytes. A saturação e o ponto de inversão:

| Condição | Vazão útil máxima | Com carga oferecida de |
| --- | --- | --- |
| síncrona, 500 B | 143,9 pedidos/s | 146,3 |
| síncrona, 5.000 B | 144,8 pedidos/s | 146,2 |
| assíncrona, 500 B | 144,2 pedidos/s | 183,9 |
| assíncrona, 5.000 B | 142,8 pedidos/s | 233,8 |

O ponto de inversão de vantagem sai em **158,7 pedidos/s oferecidos** nos dois
tamanhos. A assimetria da carga oferecida no pico é o resultado em si: a via
assíncrona continua entregando a mesma vazão enquanto a demanda cresce, porque a
fila absorve o excedente; a síncrona para de crescer e passa a recusar — 0,68 de
taxa de erro no topo da rampa, contra zero na assíncrona.

Os sete critérios do passo 8, com os limites do planejamento:

| Conferir | Critério | Medido |
| --- | --- | --- |
| Saturação entre 100 e 200 pedidos/s | 100–200 | 142,8 a 144,8 nas quatro condições |
| Produtor sem saturar | `cpu_produtor_pico` < 320 | 301 (pior caso: síncrona, 5.000 B) |
| Gerador sem saturar | `cpu_carga_pico` < 160 | 74 |
| Banco sem saturar | `cpu_postgres_pico` < 80 | 50 |
| Consumidor sem saturar | `cpu_consumidor_pct` < 80 | 28 |
| Contagem do banco | `divergencia_banco` = 0 | 0 (ver a parada do consumidor, abaixo) |
| C7 na via assíncrona | `nao_gravados` = 0 | 0 |

Os descartes do gerador ficaram em **1 a 4 por execução de 75.000 pedidos**
(0,005 %), contra 392 com o teto de 400. Em C6 o tempo de recuperação da via
assíncrona saiu em **90 s**, mensurável com folga dentro da execução; a síncrona
recupera em 1 s porque não acumula — recusa 10.763 pedidos no pico.

A latência no pico separa as duas vias na direção oposta à da hipótese: a
síncrona fica em p50 de 125 ms com 500 bytes, a assíncrona em 11 s, porque o
tempo na fila entra na medição. Abaixo da saturação a assíncrona é
marginalmente mais rápida; acima dela, muito mais lenta e sem erro. A metade da
hipótese que trata de latência não se sustenta; a que trata de taxa de erro se
sustenta com margem.

## Execução final

```powershell
# Desenho principal: C1 a C5, duas vias, dois tamanhos, três repetições
.\scripts\executar.ps1

# Complementares, apenas com 500 bytes
.\scripts\executar.ps1 -Cenarios C6,C7 -Tamanhos 500
```

São 72 execuções, cerca de 13 h 30 min: 60 do desenho principal e 12
complementares. O planejamento sugere quatro sessões, três de ~4 h, uma por
repetição, e uma de ~1 h 45 min com C6 e C7, para que uma diferença entre dias
atinja as duas vias e os dois tamanhos por igual.

A ordem das vias e dos tamanhos é sorteada por cenário com semente fixa
(`-Semente`, padrão 20261025) e registrada em `ordem_no_par` nos metadados, de
modo que a sequência seja reproduzível.

## Saídas

Por execução, em `resultados/<cenario>_<via>_<tamanho>b_r<repeticao>/`:

| Arquivo | Conteúdo |
| --- | --- |
| `consumidor.csv` | uma linha por operação: T0, T1, latência, tamanho e situação |
| `consumidor_aquecimento.csv` | dados do cenário C0, preservados como descartados |
| `produtor_serie_<pid>.csv` | emissões e erros por segundo, um arquivo por worker |
| `k6_resumo.json` | emitidos, aceitos, erros e iterações descartadas |
| `recursos.csv` | CPU e memória dos cinco contêineres, a cada ~1 s |
| `fila.csv` | mensagens prontas e não confirmadas, a cada ~1 s |
| `metadados.json` | data, versões, limites, parâmetros e linhas no banco |
| `rampa.csv` | séries por janela, geradas por `coleta/rampa.py` |
| `eventos.csv` | séries de 1 s de C6 e C7, geradas por `coleta/eventos.py` |

Consolidação:

```bash
python coleta/consolidar.py     # consolidado.csv e resumo.csv
python coleta/graficos.py       # um gráfico por variável e por tamanho
python coleta/rampa.py          # saturação e ponto de inversão, por tamanho
python coleta/eventos.py        # indicadores de C6 e C7
```

`consolidado.csv` traz uma linha por execução; `resumo.csv`, a mediana das três
repetições por via, cenário e tamanho, com a amplitude observada. Por padrão só
entram as execuções sem rótulo — as de verificação e de calibração são acessadas
por `--rotulo`.

## Decisões de implementação que afetam a leitura dos resultados

**Marco de conclusão.** T1 é tomado depois do COMMIT, nas duas vias. Na via
assíncrona a resposta ao gerador confirma apenas a aceitação da mensagem, não o
processamento.

**T0 e a espera antes dele.** T0 é registrado pelo produtor ao receber a
requisição, como o Quadro 5 define. Isso exclui da latência a espera na fila de
conexões do produtor, que sob saturação não é pequena: na verificação anterior,
com o produtor em 95 % do seu limite, o k6 media mediana de 2.356 ms enquanto o
consumidor registrava 758 ms. Por isso o protótipo grava também o instante
tomado pelo gerador e reporta `atraso_t0_p50_ms`, a diferença entre as duas
latências. **O critério do piloto é manter a CPU do produtor abaixo de 80 % do
limite**; acima disso, a latência medida subestima a via síncrona.

**Unidade de carga.** Pedidos por segundo, não usuários virtuais concorrentes.
Em modelo fechado, o usuário da via assíncrona é liberado na aceitação da
mensagem e o da síncrona só depois da gravação, de modo que o mesmo número de
usuários submete demandas muito diferentes às duas vias — na execução preliminar
sem banco, 100 usuários emitiram 7.156 requisições à via síncrona e 67.548 à
assíncrona. A coluna `descartadas_gerador` registra a demanda que o k6 não
conseguiu emitir; se for relevante, o gerador foi o componente saturado.

**Recusa por prazo vencido.** Na via síncrona, ao obter a vaga no semáforo o
consumidor verifica se ainda há tempo para concluir antes do tempo limite; se
não houver, recusa com 503 e registra `expirada`. Sem isso o consumidor seguiria
executando trabalho abandonado e a espera deixaria de ser limitada pelo tempo
limite. Não se aplica à via assíncrona, em que a mensagem espera em fila por
definição. **O texto não menciona esse mecanismo e precisa declará-lo na seção
3.2.2.**

**Taxa de erro e vazão.** A taxa de erro é medida no ponto de emissão: pedidos
emitidos sem registro T1 de sucesso, por erro HTTP, tempo esgotado, falha de
validação ou de gravação e mensagens não processadas ao fim da drenagem. Na via
síncrona a vazão útil é a das respostas que o solicitante aceitou — contar no
consumidor as conclusões dentro do prazo superestimaria a via, porque a operação
pode terminar no prazo pelo relógio do consumidor e a resposta chegar depois.
`vazao_janela` mantém a medida literal na saída do consumidor.

**Conclusões posteriores ao prazo.** Contadas nas duas vias, em `apos_limite` e
`prop_apos_limite`. Na via assíncrona é esse número, e não a taxa de erro, que
expressa a degradação.

**Tabelas esvaziadas antes da medição.** O consumidor grava cada pedido com o
identificador da execução, que é o mesmo no aquecimento e na medição. Sem
esvaziar, os ~3.000 pedidos do C0 ficariam na tabela e a contagem de linhas
superaria os registros T1 em **todas** as 72 execuções. Por isso
`POST /reiniciar-registro` executa `TRUNCATE pedidos, itens` além de rotacionar o
arquivo — o que é também o item a) do protocolo, que pede as tabelas vazias no
início da medição.

**O produtor não valida o pedido.** Quem valida é o consumidor, antes da
gravação, como o Quadro 3 atribui. Validar no produtor gastaria CPU no componente
que limita a via síncrona, tanto mais com a mensagem de 5.000 bytes, e impediria
que um pedido inválido chegasse ao consumidor — que é onde a falha de validação
deve ser contabilizada.

**Critério da inversão.** A vazão comparada é a **útil**, nas duas vias: as
respostas aceitas pelo solicitante na síncrona, as conclusões do consumidor na
assíncrona. É a mesma grandeza que a consolidação usa. A escolha não é neutra: a
recusa por prazo vencido mantém o consumidor síncrono na capacidade máxima mesmo
em sobrecarga, de modo que, medindo pelas conclusões do consumidor, a vantagem de
vazão da via assíncrona quase desaparece. As margens exigidas — 5 % de vazão e 1
ponto percentual de erro, mantidas nas janelas seguintes — são parâmetros de
`coleta/rampa.py`, impressas na saída, e **precisam ser declaradas na seção 3.5**,
porque o texto não as traz.

**Idempotência.** Pela chave primária `pedido_id`: a reentrega da mesma mensagem
não produz segunda linha e é registrada como `duplicada`. O `pedido_id` é gerado
pelo k6 de forma determinística a partir do usuário virtual e da iteração, e não
por `Math.random`, porque usuários virtuais podem partir da mesma semente e uma
colisão apareceria como reentrega.

**Tamanho da mensagem.** A estrutura do Quadro 3, com descrições longas, parte de
562 bytes serializados, acima do nível de 500. As descrições foram encurtadas
para que o piso fique em ~477 bytes e o campo de observação cubra a diferença nos
dois níveis. O roteiro do k6 falha na partida se o nível pedido ficar abaixo do
piso.

**Recursos restritos à janela de carga.** Médias e picos consideram apenas as
amostras entre o início e o fim da carga. O contêiner do gerador entra como
`cpu_carga_pct` e o do PostgreSQL como `cpu_postgres_pct`.

**Cadência da amostragem.** `docker stats` em modo contínuo entrega cerca de
1,75 amostra por segundo por contêiner, acima do uma por segundo que o Quadro 5
declara. O modo `--no-stream` levaria de 2 a 5 segundos por amostra e não
atenderia.

## Ajustes necessários no texto do artigo

1. **Quadro 4**: Docker Engine instalado é 29.8.0, não 29.8.1. Acrescentar
   uvicorn 0.54.0, httpx 0.28.1, aio-pika 10.1.0, Pydantic 2.14.1 e asyncpg
   0.32.0.
2. **Seção 3.2.2**: declarar o tempo limite adotado (1 s), a recusa por prazo
   vencido apenas na via síncrona, os 4 processos do produtor e o valor de
   `CONCORRENCIA`.
3. **Quadro 5, taxa de erro**: declarar a duração da janela de drenagem (600 s).
4. **Quadro 3**: registrar que as descrições dos itens são curtas, para que o
   nível de 500 bytes seja alcançável.
5. **Seção 3.6**: acrescentar que o produtor em processo único não passa de um
   núcleo, de modo que o número de processos é parte do dimensionamento; e que o
   hospedeiro, e não o produtor, foi o recurso limitante em sobrecarga na
   verificação anterior.
6. **Quadro 6**: o teto de carga passa de 400 para **250 pedidos/s** em C4, C5 e
   C6. Acima de 248 pedidos/s oferecidos a via síncrona com 500 bytes leva o
   produtor ao seu limite de CPU — de 239 % para 418 % de uma janela para a
   seguinte — e o gerador deixa de emitir toda a demanda, de modo que a medição
   passaria a refletir o dimensionamento do equipamento, e não o estilo de
   integração (seção 3.6). A saturação, em 143 pedidos/s, e o ponto de inversão,
   em 159, continuam inteiramente cobertos. Com o teto menor C6 volta a durar
   cerca de 6 min, como o quadro já declara: o minuto de pico acumula ~6.800
   mensagens, que escoam a cerca de 64 por segundo.
7. **Seção 3.5**: declarar qual vazão é comparada no ponto de inversão (a útil) e
   as margens adotadas, ou substituí-las pela amplitude das três repetições.
8. **Seção 3.4, item a)**: registrar que as tabelas são esvaziadas também depois
   do aquecimento, e não apenas na subida do ambiente.

## Correções da 2ª rodada de verificação

| Ponto | Correção |
| --- | --- |
| Aquecimento gravava no banco com o identificador da medição | `TRUNCATE pedidos, itens` no `/reiniciar-registro` |
| Regra de inversão dependia de escolhas fora do texto | `rampa.py` passa a usar a vazão útil; margens explícitas e impressas |
| C6 terminava antes de a fila esvaziar | cauda estendida a 4 min (`C6_CAUDA`), execução de ~6 min |
| Produtor validava o pedido inteiro | validação removida do produtor |
| C7 gravava o retorno pelo `compose start` | passa a aguardar o `/saude` do consumidor |
| Pydantic sem versão fixa | fixado em 2.14.1 nos dois `requirements.txt` |
| Pasta `carga/k6;C` | removida (estava vazia; artefato de chamar o Docker pelo Git Bash) |
| Contêiner do k6 com nome fixo | `docker rm -f` antes de cada execução |

Durante essas correções apareceu um defeito que não estava na lista: **o ambiente
falhava em cerca de uma de cada três subidas**. O `rabbitmq-diagnostics ping`
responde assim que o nó Erlang sobe, antes de a porta 5672 aceitar conexões, e os
serviços encerravam com `connection refused`. O healthcheck passou a usar
`check_port_connectivity`, o do PostgreSQL passou a conferir por TCP e a
consultar a tabela, e as conexões iniciais com o banco e com o intermediador
ganharam novas tentativas. Quatro subidas consecutivas limpas depois disso.

Verificação da correção do aquecimento, com C0 ativo: 3.001 pedidos emitidos,
3.001 concluídos e **3.001 linhas no banco** — antes, as ~500 linhas do
aquecimento entrariam na contagem. Em C6 com a cauda curta: 30.999 emitidos,
concluídos e gravados, `divergencia_banco` igual a zero, fila no pico de 18.644
mensagens e PostgreSQL a 21 % de CPU, com pico de 52 %.

## A parada do consumidor perdia registros

Em C7 a contagem do banco não fechava com os registros T1: 19 linhas a mais na
via síncrona e 8 na assíncrona, em cerca de 30.000. Não era erro de medição, era
perda de dados. Os dois serviços subiam com `CMD ["sh", "-c", "uvicorn ..."]`, e
sem `exec` o `sh` permanece como processo 1 e **não repassa o SIGTERM**: o
`docker compose stop` esperava a carência e matava o contêiner com SIGKILL (saída
137), de modo que o encerramento do serviço — que descarrega o buffer de
registros — nunca ocorria. As linhas faltantes são exatamente os 0,2 s de
`INTERVALO_FLUSH_S` pendentes no instante da parada.

Isolado num teste só com uma parada no meio da carga, sem aquecimento e sem C7, o
defeito dava divergência de 9 linhas. Com `exec` no comando e
`--timeout-graceful-shutdown`, o processo 1 passa a ser o uvicorn, a saída passa a
ser 0 e a divergência vai a zero. C7 refeito nas duas vias:

| | assíncrona | síncrona |
| --- | --- | --- |
| Linhas no banco / registros T1 | 30.001 / 30.001 | 26.579 / 26.579 |
| Divergência | **0** | **0** |
| Recusados ou perdidos no evento | 0 | 3.252 |
| Tempo de recuperação | 91 s | 3 s |

O mesmo comando afetava o produtor: o último balde parcial da série por segundo
se perdia em toda execução. Fica fora da janela de medição, mas estava errado do
mesmo modo.

## C6 completo com o teto de 400 (histórico)

Esta execução é anterior ao teto de 250; os números atuais de C6 estão na seção
do teste-piloto.


Execução única de C6 na via assíncrona, com aquecimento, após ajustar a cauda:

| | |
| --- | --- |
| Emitidos / concluídos / linhas no banco | 44.999 / 44.999 / 44.999 |
| Divergência do banco | 0 |
| Descartados pelo gerador | 0 |
| Ocupação máxima da fila | 19.894 |
| Recusados ou perdidos no pico | 0 |
| **Tempo de recuperação** | **255 s** |
| CPU: produtor / PostgreSQL / gerador (pico) | 222 % de 400 % / 61 % de 100 % / 89 % de 200 % |

Durante essa verificação apareceu um defeito que travava o script logo após subir
o ambiente, em qualquer execução não interativa. A causa era o próprio
`.wslconfig`: o `ConvertTo-Json` do Windows PowerShell 5.1 **trava
indefinidamente** ao receber uma string com quebras de linha fora de uma sessão
interativa, e o conteúdo do arquivo entrava cru nos metadados. A mesma string de
32 caracteres converte em 0,1 s quando as linhas são unidas. O script passou a
unir as linhas, e ganhou também um `progresso.log` por execução, com marcação de
tempo por etapa — foi ele que localizou a linha, e serve para acompanhar as
sessões de 4 h.

## Ao mover a pasta para fora do OneDrive

A pasta de medição precisa ficar fora do OneDrive, que sincroniza durante a
execução e consome CPU do hospedeiro. Mas mover não basta: os arquivos saem
levando a marca de nova análise (*reparse point*) do OneDrive, de tipo
`0x9000201a`. O conteúdo fica local e qualquer programa do Windows lê sem
problema, porém o BuildKit roda dentro do WSL 2 e não atravessa a marca:

```
failed to read dockerfile: invalid file request Dockerfile
```

O erro aparece no `docker compose build` e aponta para arquivos arbitrários do
contexto, o que esconde a causa. Para converter os arquivos em arquivos comuns,
depois de mover:

```powershell
Get-ChildItem -Recurse -File -Force |
  Where-Object { $_.FullName -notmatch '\resultados\' -and
                 ($_.Attributes -band [IO.FileAttributes]::ReparsePoint) } |
  ForEach-Object {
      $b = [IO.File]::ReadAllBytes($_.FullName)
      [IO.File]::WriteAllBytes($_.FullName + '.novo', $b)
      Remove-Item -LiteralPath $_.FullName -Force
      Rename-Item -LiteralPath ($_.FullName + '.novo') -NewName $_.Name
  }
```

Confira com `Get-ChildItem -Recurse -File -Force | Where-Object { $_.Attributes
-band [IO.FileAttributes]::ReparsePoint }`, que deve não devolver nada, e refaça
o `docker compose build`. Os 30 arquivos de código foram convertidos com o
conteúdo conferido por SHA-256 antes e depois.

## Pendências

- **Ajustes no texto do artigo**, listados acima.
- **Repositório público**, que a seção 3.6 promete. Depende de decisão do grupo
  sobre onde publicar.
- **Ambiente de medição**: copiar a pasta para um caminho simples, registrar o
  `.wslconfig` e rodar na tomada. O OneDrive sincroniza esta pasta durante a
  medição, o que consome CPU do hospedeiro.
- **Execução final** das 72 medições.
