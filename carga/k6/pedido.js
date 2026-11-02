// Gerador de carga do experimento. Cenarios conforme o Quadro 6 do artigo,
// com progressao adaptada de Shafabakhsh, Lagerstrom e Hacks (2020).
// Execucao: um cenario, uma via e um tamanho por invocacao, selecionados por
// variavel de ambiente, de modo que as execucoes possam ser intercaladas
// (item d, secao 3.4).
//
// A via nao e escolhida aqui: o produtor expoe um unico POST /pedidos e decide
// pelo seu proprio parametro de configuracao (Quadro 5). MODO serve apenas para
// marcar as metricas e para saber qual resposta esperar.
import http from 'k6/http';
import { Counter, Trend } from 'k6/metrics';
import { montarPedido, pisoDoPedido } from './payload.js';

const MODO = (__ENV.MODO || 'sincrono').toLowerCase();
const CENARIO = (__ENV.CENARIO || 'C1').toUpperCase();
const BASE_URL = __ENV.BASE_URL || 'http://produtor:8000';
const TEMPO_LIMITE_MS = parseInt(__ENV.TEMPO_LIMITE_MS || '1000', 10);
const TAMANHO_MENSAGEM = parseInt(__ENV.TAMANHO_MENSAGEM || '500', 10);
const EXECUCAO_ID = __ENV.EXECUCAO_ID || 'avulsa';
// Semente do identificador do pedido: unica por execucao, porque as tabelas do
// banco sao esvaziadas entre cenarios e a chave primaria e o pedido_id.
const SEMENTE = parseInt(__ENV.SEMENTE || String(Date.now() % 0xffffffff), 10);

// Quadro 6 - Cenarios de carga, em pedidos por segundo. A carga e definida por
// taxa de chegada, mantida pelo gerador independentemente do tempo de resposta:
// em modelo fechado, o usuario virtual da via assincrona e liberado na aceitacao
// da mensagem e o da sincrona apenas depois da gravacao, de modo que o mesmo
// numero de usuarios submeteria demandas muito diferentes as duas vias
// (Schroeder; Wierman; Harchol-Balter, 2006).
const CENARIOS = {
  C0: { executor: 'constant-arrival-rate', rate: 25, duration: '2m' },
  C1: { executor: 'constant-arrival-rate', rate: 50, duration: '5m' },
  C2: { executor: 'constant-arrival-rate', rate: 100, duration: '5m' },
  C3: { executor: 'constant-arrival-rate', rate: 200, duration: '5m' },
  C4: { executor: 'constant-arrival-rate', rate: 250, duration: '5m' },
  C5: {
    executor: 'ramping-arrival-rate',
    startRate: 0,
    stages: [{ duration: '10m', target: 250 }],
  },
  // C6 - pico subito: 50 pedidos/s, sobe para 250 em 10 s, 1 min no pico e
  // retorna a 50, mantendo-se assim ate o fim da execucao.
  //
  // A cauda de 50 pedidos/s e dimensionada para que a fila se esvazie dentro da
  // execucao: sem isso o tempo de recuperacao, um dos tres indicadores da secao
  // 3.5, nao e mensuravel, porque exige vazao e fila de volta ao patamar ao
  // mesmo tempo. Com pico de 250 o minuto de sobrecarga acumula cerca de 6.800
  // mensagens, que escoam a cerca de 64 por segundo, perto de 110 s. A cauda de
  // 4 min cobre isso com folga e mantem a execucao em cerca de 6 min.
  C6: {
    executor: 'ramping-arrival-rate',
    startRate: 50,
    stages: [
      { duration: '30s', target: 50 },
      { duration: '10s', target: 250 },
      { duration: '1m', target: 250 },
      { duration: '10s', target: 50 },
      { duration: __ENV.C6_CAUDA || '4m', target: 50 },
    ],
  },
  // C7 - queda temporaria: carga constante de 100 pedidos/s por 5 min. A
  // interrupcao do consumidor aos 2 min e conduzida pelo script de execucao.
  C7: { executor: 'constant-arrival-rate', rate: 100, duration: '5m' },
};

if (!CENARIOS[CENARIO]) {
  throw new Error(`cenario desconhecido: ${CENARIO}`);
}

// O tamanho da mensagem e variavel independente: se o nivel pedido for menor que
// o pedido sem preenchimento, as execucoes mediriam um tamanho que nao e o
// declarado. Melhor falhar na partida que medir errado.
const PISO = pisoDoPedido();
if (TAMANHO_MENSAGEM < PISO) {
  throw new Error(
    `TAMANHO_MENSAGEM=${TAMANHO_MENSAGEM} abaixo do piso de ${PISO} bytes do pedido do Quadro 3`
  );
}

// Sobrescritas usadas pelo teste-piloto de calibracao e pela verificacao rapida
// do prototipo (secao 3.4). Nas execucoes finais nao sao definidas.
const cenario = Object.assign({}, CENARIOS[CENARIO]);
const porEstagios = cenario.executor === 'ramping-arrival-rate';
if (__ENV.CARGA) {
  const carga = parseInt(__ENV.CARGA, 10);
  if (porEstagios) {
    const maior = Math.max(...cenario.stages.map((e) => e.target));
    const fator = maior > 0 ? carga / maior : 1;
    cenario.startRate = Math.round(cenario.startRate * fator);
    cenario.stages = cenario.stages.map((e) =>
      Object.assign({}, e, { target: Math.round(e.target * fator) })
    );
  } else {
    cenario.rate = carga;
  }
}
if (__ENV.DURACAO) {
  if (porEstagios) {
    // Encurta a rampa mantendo a proporcao entre os estagios.
    cenario.stages = [{ duration: __ENV.DURACAO, target: Math.max(...cenario.stages.map((e) => e.target)) }];
  } else {
    cenario.duration = __ENV.DURACAO;
  }
}

// Reserva de usuarios virtuais. Nos executores por taxa de chegada eles sao
// apenas o meio de emitir as requisicoes: a reserva precisa cobrir o pior caso,
// em que cada requisicao ocupa um usuario virtual pelo tempo limite inteiro.
// Alocar durante o teste e o que produz iteracoes descartadas, isto e, demanda
// que o gerador deixou de emitir (secao 3.6).
const alvo = porEstagios
  ? Math.max(...cenario.stages.map((e) => e.target), cenario.startRate || 0)
  : cenario.rate;
const segundosLimite = Math.max(1, TEMPO_LIMITE_MS / 1000);
cenario.timeUnit = '1s';
cenario.preAllocatedVUs = Math.max(50, Math.ceil(alvo * segundosLimite));
cenario.maxVUs = Math.max(cenario.preAllocatedVUs, Math.ceil(alvo * segundosLimite * 2));

export const options = {
  scenarios: {
    [CENARIO]: Object.assign({}, cenario, {
      tags: { cenario: CENARIO, modo: MODO, tamanho: String(TAMANHO_MENSAGEM) },
    }),
  },
  discardResponseBodies: true,
  noConnectionReuse: false,
  summaryTrendStats: ['avg', 'min', 'med', 'p(95)', 'p(99)', 'max'],
};

// A latencia de referencia do experimento vai de T0 a T1 e e montada a partir
// dos registros do produtor e do consumidor (secao 3.3). As metricas abaixo
// caracterizam o lado do gerador: pedidos emitidos, aceitos e recusados, e o
// tempo de resposta do produtor, que serve de conferencia.
const emitidas = new Counter('exp_emitidas');
const aceitas = new Counter('exp_aceitas');
const erros_http = new Counter('exp_erros_http');
const erros_tempo_limite = new Counter('exp_erros_tempo_limite');
const resposta_produtor = new Trend('exp_resposta_produtor_ms', true);
const tamanho_corpo = new Trend('exp_tamanho_corpo_bytes');

const ESPERADO = MODO === 'assincrono' ? 202 : 200;

export default function () {
  const corpo = montarPedido(TAMANHO_MENSAGEM, __VU, __ITER, SEMENTE);
  const emitidoEmMs = Date.now();
  const resposta = http.post(`${BASE_URL}/pedidos`, corpo, {
    headers: {
      'Content-Type': 'application/json',
      // Conferencia do instante de emissao: T0 e tomado pelo produtor, e a
      // diferenca entre os dois mede a espera na fila de conexoes.
      'x-emitido-em-ms': String(emitidoEmMs),
    },
    timeout: `${TEMPO_LIMITE_MS}ms`,
    tags: { cenario: CENARIO, modo: MODO, tamanho: String(TAMANHO_MENSAGEM) },
  });

  emitidas.add(1);
  tamanho_corpo.add(corpo.length);
  resposta_produtor.add(resposta.timings.duration);

  if (resposta.status === ESPERADO) {
    aceitas.add(1);
  } else if (resposta.status === 0 || resposta.status === 504) {
    // Tempo esgotado: contabilizado como erro, e nao como latencia elevada.
    erros_tempo_limite.add(1);
  } else {
    erros_http.add(1);
  }
}

export function handleSummary(dados) {
  // dropped_iterations, no resumo, e demanda que o gerador nao conseguiu emitir.
  // Precisa ser lida na analise: nao e erro do servico, e limite do gerador.
  const destino = `/resultados/${EXECUCAO_ID}/k6_resumo.json`;
  const saida = {};
  saida[destino] = JSON.stringify(
    {
      cenario: CENARIO,
      modo: MODO,
      tamanho_mensagem: TAMANHO_MENSAGEM,
      execucao_id: EXECUCAO_ID,
      resumo: dados,
    },
    null,
    2
  );
  return saida;
}
