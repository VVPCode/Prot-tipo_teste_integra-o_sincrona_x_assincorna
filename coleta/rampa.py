"""Analise do cenario de rampa: ponto de saturacao e ponto de inversao.

Atende duas exigencias do artigo que a consolidacao por execucao nao cobre, por
reduzir cada execucao a uma mediana:

  - secao 3.4: localizar o nivel de carga em que o servico consumidor satura,
    para verificar se os cenarios do Quadro 6 o mantem no interior do intervalo;
  - secao 3.5: localizar o ponto de inversao de vantagem, definido como o
    primeiro nivel de carga em que a via assincrona apresenta simultaneamente
    maior vazao e menor taxa de erro que a sincrona, separadamente para cada
    tamanho de mensagem.

A vazao comparada e a util: na via sincrona, as respostas que o solicitante
aceitou; na assincrona, as conclusoes registradas pelo consumidor, ja que ali o
solicitante aguardou apenas a aceitacao da mensagem. E a mesma grandeza que a
consolidacao usa. Contar na via sincrona as conclusoes do consumidor incluiria o
que chegou tarde ao solicitante, e a recusa por prazo vencido mantem o consumidor
sincrono na capacidade maxima mesmo em sobrecarga, de modo que a escolha da
grandeza decide o resultado.

As margens exigidas para reconhecer a inversao sao parametros, impressos na
saida, porque nao constam do texto e precisam ser declaradas na secao 3.5.

A rampa e dividida em janelas de tempo. A vazao e a latencia vem do consumidor,
que registra T1 (secao 3.3); os pedidos emitidos e os erros vem da serie do
produtor, pois pedidos recusados ou com tempo esgotado nunca chegam ao
consumidor.

Uso:
  python coleta/rampa.py --dir resultados --rotulo piloto
  python coleta/rampa.py --execucao resultados/C5_sincrono_500b_r1
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os

COLUNAS = [
    "janela_s", "carga_alvo_ops", "emitidas_s", "vazao_util_s", "vazao_janela_s",
    "taxa_erro", "erros_tempo_limite_s", "erros_http_s", "erros_publicacao_s",
    "lat_p50_ms", "lat_p95_ms", "lat_p99_ms", "concluidas", "aceitas",
]


def percentil(valores, p):
    if not valores:
        return ""
    ordenados = sorted(valores)
    k = int(round(p / 100.0 * len(ordenados) + 0.5)) - 1
    k = max(0, min(len(ordenados) - 1, k))
    return round(ordenados[k], 3)


def ler_metadados(pasta):
    caminho = os.path.join(pasta, "metadados.json")
    if not os.path.exists(caminho):
        return None
    with open(caminho, encoding="utf-8") as f:
        return json.load(f)


def contar_janelas(duracao_s, largura_s):
    """Numero de janelas. A sobra final so entra se cobrir metade da largura.

    Uma janela muito curta receberia as conclusoes do encerramento da carga e,
    dividida por sua largura, produziria uma taxa artificialmente alta.
    """
    cheias = int(duracao_s / largura_s)
    sobra = duracao_s - cheias * largura_s
    return max(1, cheias + (1 if sobra >= 0.5 * largura_s else 0))


def series(pasta, inicio_ms, fim_ms, largura_s):
    """Agrega consumidor.csv e a serie do produtor em janelas de largura fixa."""
    n = contar_janelas((fim_ms - inicio_ms) / 1000.0, largura_s)
    latencias = [[] for _ in range(n)]
    concluidas = [0] * n
    emitidas = [0] * n
    erros = {chave: [0] * n for chave in
             ("erro_tempo_limite", "erro_http", "erro_publicacao", "aceita")}

    def indice(instante_ms):
        posicao = int((instante_ms - inicio_ms) / 1000.0 / largura_s)
        return posicao if 0 <= posicao < n else None

    caminho = os.path.join(pasta, "consumidor.csv")
    if os.path.exists(caminho):
        with open(caminho, encoding="utf-8", newline="") as f:
            leitor = csv.DictReader(f)
            # Execucoes anteriores a entrada do PostgreSQL gravavam
            # concluido_em_ms em lugar de t1_ms: ficam de fora, com aviso.
            if leitor.fieldnames and "t1_ms" not in leitor.fieldnames:
                return None
            for linha in leitor:
                if linha["situacao"] != "sucesso":
                    continue
                i = indice(float(linha["t1_ms"]))
                if i is None:
                    continue
                concluidas[i] += 1
                lat = float(linha["latencia_ms"])
                if lat >= 0:
                    latencias[i].append(lat)

    # O produtor grava um arquivo por worker do uvicorn. O aquecimento nao
    # precisa ser descartado: as linhas fora da janela de medicao ficam de fora.
    for caminho in sorted(glob.glob(os.path.join(pasta, "produtor_serie_*.csv"))):
        if "aquecimento" in os.path.basename(caminho):
            continue
        with open(caminho, encoding="utf-8", newline="") as f:
            for linha in csv.DictReader(f):
                i = indice(float(linha["instante_s"]) * 1000.0)
                if i is None:
                    continue
                emitidas[i] += int(linha["emitidas"])
                for chave in erros:
                    erros[chave][i] += int(linha.get(chave, 0) or 0)

    return {
        "janelas": n, "latencias": latencias, "concluidas": concluidas,
        "emitidas": emitidas, "erros": erros,
    }


def montar(pasta, largura_s):
    meta = ler_metadados(pasta)
    if meta is None:
        return None
    inicio_ms = float(meta.get("inicio_carga_ms", 0) or 0)
    fim_ms = float(meta.get("fim_carga_ms", 0) or 0)
    if not inicio_ms or fim_ms <= inicio_ms:
        return None

    alvo = float(meta.get("carga_alvo_ops", 0) or 0)
    duracao_s = (fim_ms - inicio_ms) / 1000.0
    cenario = (meta.get("cenario") or "").upper()
    modo = (meta.get("modo") or "").lower()
    rampa = cenario == "C5"

    dados = series(pasta, inicio_ms, fim_ms, largura_s)
    if dados is None:
        return "incompativel"
    n = dados["janelas"]
    linhas = []
    for i in range(n):
        # A ultima janela pode ser parcial: as taxas usam a largura efetiva.
        largura = min(largura_s, duracao_s - i * largura_s)
        meio_s = i * largura_s + largura / 2.0
        # Na rampa a taxa de chegada cresce linearmente; nos demais e constante.
        carga = round(alvo * min(1.0, meio_s / duracao_s), 1) if rampa else alvo
        emitidas = dados["emitidas"][i]
        concluidas = dados["concluidas"][i]
        # Medida no ponto de emissao: um pedido com tempo esgotado no
        # solicitante e erro, ainda que o consumidor o termine depois.
        nao_concluidas = (dados["erros"]["erro_tempo_limite"][i]
                          + dados["erros"]["erro_http"][i]
                          + dados["erros"]["erro_publicacao"][i])
        # Vazao util: na via sincrona sao as respostas aceitas pelo solicitante;
        # na assincrona, as conclusoes no consumidor.
        aceitas = dados["erros"]["aceita"][i]
        uteis = aceitas if modo == "sincrono" else concluidas
        linhas.append({
            "janela_s": round(i * largura_s + largura, 1),
            "carga_alvo_ops": carga,
            "emitidas_s": round(emitidas / largura, 2),
            "vazao_util_s": round(uteis / largura, 2),
            "vazao_janela_s": round(concluidas / largura, 2),
            "taxa_erro": round(nao_concluidas / emitidas, 4) if emitidas else "",
            "erros_tempo_limite_s": round(dados["erros"]["erro_tempo_limite"][i] / largura, 2),
            "erros_http_s": round(dados["erros"]["erro_http"][i] / largura, 2),
            "erros_publicacao_s": round(dados["erros"]["erro_publicacao"][i] / largura, 2),
            "lat_p50_ms": percentil(dados["latencias"][i], 50),
            "lat_p95_ms": percentil(dados["latencias"][i], 95),
            "lat_p99_ms": percentil(dados["latencias"][i], 99),
            "concluidas": concluidas,
            "aceitas": aceitas,
        })

    destino = os.path.join(pasta, "rampa.csv")
    with open(destino, "w", encoding="utf-8", newline="") as f:
        escritor = csv.DictWriter(f, fieldnames=COLUNAS)
        escritor.writeheader()
        escritor.writerows(linhas)

    return {
        "pasta": pasta,
        "modo": modo,
        "cenario": cenario,
        "tamanho": str(meta.get("tamanho_mensagem", "")),
        "rampa": rampa,
        "linhas": linhas,
        "destino": destino,
    }


def saturacao(linhas, tolerancia=0.05):
    """Janela em que a vazao atinge seu maximo.

    A saturacao e a condicao em que a taxa de chegada supera a de atendimento e a
    vazao deixa de crescer com o aumento da carga (secao 2.5).
    """
    com_vazao = [l for l in linhas if l["vazao_util_s"] > 0]
    if len(com_vazao) < 4:
        return None
    pico = max(com_vazao, key=lambda l: l["vazao_util_s"])
    limiar = pico["vazao_util_s"] * (1.0 - tolerancia)
    dentro = [l for l in com_vazao if l["vazao_util_s"] >= limiar]
    return {"pico": pico, "ultima_no_patamar": dentro[-1] if dentro else pico}


def inversao(serie_sinc, serie_assinc, margem_vazao=0.05, margem_erro=0.01,
             exigir_persistencia=True):
    """Primeiro nivel de carga em que a via assincrona leva vantagem nas duas
    grandezas ao mesmo tempo (secao 3.5).

    O criterio do artigo e simultaneo: maior vazao e menor taxa de erro. A
    diferenca precisa superar uma margem e persistir nas janelas seguintes, pois
    sob carga baixa as duas vias entregam praticamente a mesma coisa e a
    alternancia entre elas e apenas variacao entre janelas.
    """
    pares = min(len(serie_sinc["linhas"]), len(serie_assinc["linhas"]))

    def vantagem(i):
        a, b = serie_sinc["linhas"][i], serie_assinc["linhas"][i]
        maior_vazao = b["vazao_util_s"] > a["vazao_util_s"] * (1.0 + margem_vazao)
        ea, eb = a["taxa_erro"], b["taxa_erro"]
        menor_erro = ea != "" and eb != "" and ea > eb + margem_erro
        return maior_vazao and menor_erro

    for i in range(pares):
        if not vantagem(i):
            continue
        if exigir_persistencia and not all(vantagem(j) for j in range(i, pares)):
            continue
        return serie_sinc["linhas"][i], serie_assinc["linhas"][i]
    return None


def relatar(serie):
    print("")
    print(f"{serie['modo']} ({serie['cenario']}, {serie['tamanho']} bytes) -> {serie['destino']}")
    achado = saturacao(serie["linhas"])
    if achado is None:
        print("  patamar de vazao nao identificado nesta faixa de carga")
    else:
        pico = achado["pico"]
        print(f"  vazao util maxima: {pico['vazao_util_s']} pedidos/s "
              f"(na saida do consumidor: {pico['vazao_janela_s']})")
        if serie["rampa"]:
            print(f"  atingida com carga oferecida de {pico['carga_alvo_ops']} pedidos/s; "
                  f"acima dessa taxa de chegada a vazao deixa de crescer")
        print(f"  latencia no pico: p50 {pico['lat_p50_ms']} ms, p95 {pico['lat_p95_ms']} ms")
        fim = serie["linhas"][-1]
        if fim["vazao_util_s"] and pico["vazao_util_s"]:
            queda = 1.0 - fim["vazao_util_s"] / pico["vazao_util_s"]
            if queda > 0.05:
                print(f"  ao fim da rampa a vazao esta {queda:.0%} abaixo do pico")
    if not any(l["emitidas_s"] for l in serie["linhas"]):
        print("  taxa de erro indisponivel: serie do produtor ausente nesta execucao")
        return
    pico_erro = max(
        (l for l in serie["linhas"] if l["taxa_erro"] != ""),
        key=lambda l: l["taxa_erro"], default=None)
    if pico_erro and pico_erro["taxa_erro"] > 0:
        print(f"  taxa de erro maxima: {pico_erro['taxa_erro']} "
              f"com carga oferecida de {pico_erro['carga_alvo_ops']} pedidos/s")
    else:
        print("  nenhum erro registrado na faixa examinada")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dir", default="resultados")
    p.add_argument("--rotulo", default=None, help="rotulo das execucoes a analisar")
    p.add_argument("--execucao", action="append", default=[],
                   help="pasta de uma execucao; pode ser repetido")
    p.add_argument("--janela", type=float, default=15.0, help="largura da janela em segundos")
    p.add_argument("--margem-vazao", type=float, default=0.05,
                   help="vantagem minima de vazao para reconhecer a inversao")
    p.add_argument("--margem-erro", type=float, default=0.01,
                   help="vantagem minima de taxa de erro, em proporcao")
    p.add_argument("--sem-persistencia", action="store_true",
                   help="aceita a inversao sem exigir que a vantagem se mantenha")
    args = p.parse_args()

    pastas = list(args.execucao)
    if not pastas:
        for nome in sorted(os.listdir(args.dir)):
            pasta = os.path.join(args.dir, nome)
            if not os.path.isdir(pasta):
                continue
            meta = ler_metadados(pasta)
            if meta is None:
                continue
            if args.rotulo is not None and (meta.get("rotulo") or "") != args.rotulo:
                continue
            pastas.append(pasta)

    series_montadas = []
    for pasta in pastas:
        serie = montar(pasta, args.janela)
        if serie == "incompativel":
            print("esquema anterior ao PostgreSQL, fora da analise: " + pasta)
            continue
        if serie is None:
            print("sem metadados utilizaveis: " + pasta)
            continue
        series_montadas.append(serie)
        relatar(serie)

    if not series_montadas:
        print("nenhuma execucao analisada")
        return 1

    # Secao 3.5: a analise e feita separadamente para cada tamanho de mensagem.
    tamanhos = sorted({s["tamanho"] for s in series_montadas})
    for tamanho in tamanhos:
        sincronas = [s for s in series_montadas if s["modo"] == "sincrono" and s["tamanho"] == tamanho]
        assincronas = [s for s in series_montadas if s["modo"] == "assincrono" and s["tamanho"] == tamanho]
        if not (sincronas and assincronas):
            continue
        print("")
        print(f"Ponto de inversao de vantagem com {tamanho} bytes (secao 3.5)")
        print(f"  criterio: vazao util {args.margem_vazao:.0%} maior e taxa de erro "
              f"{args.margem_erro:.0%} menor" + ("" if args.sem_persistencia
                                                 else ", mantidas nas janelas seguintes"))
        achado = inversao(sincronas[0], assincronas[0], args.margem_vazao,
                          args.margem_erro, not args.sem_persistencia)
        if achado is None:
            print("  a via assincrona nao apresentou, ao mesmo tempo, maior vazao e")
            print("  menor taxa de erro na faixa examinada")
        else:
            a, b = achado
            print(f"  a partir de {a['carga_alvo_ops']} pedidos/s oferecidos:")
            print(f"    vazao util {a['vazao_util_s']} (sincrona) contra "
                  f"{b['vazao_util_s']} (assincrona)")
            print(f"    taxa erro  {a['taxa_erro']} (sincrona) contra {b['taxa_erro']} (assincrona)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
