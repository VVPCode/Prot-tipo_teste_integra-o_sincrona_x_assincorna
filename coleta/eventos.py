"""Analise dos cenarios complementares C6 e C7 (secao 3.5 do artigo).

Os dois cenarios nao mantem carga constante e, por isso, nao integram a
avaliacao da hipotese: recebem analise descritiva propria. Para cada execucao,
a vazao, a taxa de erro, a latencia e a ocupacao da fila sao organizadas em
series temporais com resolucao de um segundo, e delas se extraem tres
indicadores:

  - pedidos recusados ou perdidos durante o evento;
  - latencia maxima observada;
  - tempo de recuperacao, isto e, o intervalo entre o fim do pico ou o retorno
    do consumidor e o momento em que a vazao e a ocupacao da fila voltam ao
    patamar anterior ao evento.

O evento e delimitado de forma diferente nos dois cenarios. Em C7 o script de
execucao grava os instantes da interrupcao e do retorno do consumidor nos
metadados. Em C6 o evento e o pico de carga, localizado na propria serie de
pedidos emitidos.

Uso:
  python coleta/eventos.py --dir resultados
  python coleta/eventos.py --execucao resultados/C7_assincrono_500b_r1
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import statistics

COLUNAS = [
    "segundo", "instante_ms", "fase", "emitidas_s", "aceitas_s", "erros_s",
    "vazao_s", "lat_p50_ms", "lat_max_ms", "fila_mensagens", "fila_nao_confirmadas",
]


def ler_metadados(pasta):
    caminho = os.path.join(pasta, "metadados.json")
    if not os.path.exists(caminho):
        return None
    with open(caminho, encoding="utf-8") as f:
        return json.load(f)


def _serie_consumidor(pasta, inicio_s, fim_s):
    """Conclusoes por segundo e latencias de cada segundo."""
    concluidas = {}
    latencias = {}
    caminho = os.path.join(pasta, "consumidor.csv")
    if not os.path.exists(caminho):
        return concluidas, latencias
    with open(caminho, encoding="utf-8", newline="") as f:
        for linha in csv.DictReader(f):
            if linha["situacao"] != "sucesso":
                continue
            segundo = int(float(linha["t1_ms"]) / 1000.0)
            if segundo < inicio_s or segundo > fim_s:
                continue
            concluidas[segundo] = concluidas.get(segundo, 0) + 1
            lat = float(linha["latencia_ms"])
            if lat >= 0:
                latencias.setdefault(segundo, []).append(lat)
    return concluidas, latencias


def _serie_produtor(pasta, inicio_s, fim_s):
    """Emissoes, aceitacoes e erros por segundo, no ponto de emissao."""
    emitidas, aceitas, erros = {}, {}, {}
    for caminho in sorted(glob.glob(os.path.join(pasta, "produtor_serie_*.csv"))):
        if "aquecimento" in os.path.basename(caminho):
            continue
        with open(caminho, encoding="utf-8", newline="") as f:
            for linha in csv.DictReader(f):
                segundo = int(float(linha["instante_s"]))
                if segundo < inicio_s or segundo > fim_s:
                    continue
                emitidas[segundo] = emitidas.get(segundo, 0) + int(linha["emitidas"])
                aceitas[segundo] = aceitas.get(segundo, 0) + int(linha.get("aceita", 0) or 0)
                soma = (int(linha.get("erro_tempo_limite", 0) or 0)
                        + int(linha.get("erro_http", 0) or 0)
                        + int(linha.get("erro_publicacao", 0) or 0))
                erros[segundo] = erros.get(segundo, 0) + soma
    return emitidas, aceitas, erros


def _serie_fila(pasta, inicio_s, fim_s):
    """Ocupacao da fila por segundo, com o ultimo valor repetido nas lacunas.

    A amostragem nao garante uma leitura por segundo exato. Deixar o segundo sem
    valor faria o indicador de recuperacao interpreta-lo como fila vazia e
    disparar antes do tempo, de modo que a lacuna recebe a ultima leitura.
    """
    mensagens, nao_confirmadas = {}, {}
    caminho = os.path.join(pasta, "fila.csv")
    if not os.path.exists(caminho):
        return mensagens, nao_confirmadas
    lidas = {}
    with open(caminho, encoding="utf-8", newline="") as f:
        for linha in csv.DictReader(f):
            if linha["fila"] != "pedidos.novos":
                continue
            segundo = int(float(linha["instante_ms"]) / 1000.0)
            lidas[segundo] = (int(linha["mensagens"]), int(linha["nao_confirmadas"]))
    if not lidas:
        return mensagens, nao_confirmadas
    ultimo = None
    for segundo in range(min(lidas), fim_s + 1):
        if segundo in lidas:
            ultimo = lidas[segundo]
        if ultimo is None or segundo < inicio_s:
            continue
        mensagens[segundo], nao_confirmadas[segundo] = ultimo
    return mensagens, nao_confirmadas


def _fases_c7(meta, inicio_s):
    """Instantes da interrupcao e do retorno do consumidor, gravados pelo script."""
    inicio = meta.get("queda_inicio_ms")
    fim = meta.get("queda_fim_ms")
    if not inicio or not fim:
        return None
    return int(float(inicio) / 1000.0) - inicio_s, int(float(fim) / 1000.0) - inicio_s


def _fases_c6(linhas, limiar=1.5):
    """Inicio e fim do pico, localizados na serie de pedidos emitidos.

    O pico de C6 vai de 50 a 250 pedidos/s. Define-se como pico a faixa em que a
    emissao supera a mediana do inicio da execucao por um fator.
    """
    emissoes = [l["emitidas_s"] for l in linhas if l["emitidas_s"] > 0]
    if len(emissoes) < 10:
        return None
    base = statistics.median(emissoes[: max(5, len(emissoes) // 6)])
    if base <= 0:
        return None
    acima = [l["segundo"] for l in linhas if l["emitidas_s"] > base * limiar]
    if not acima:
        return None
    return min(acima), max(acima)


def montar(pasta):
    meta = ler_metadados(pasta)
    if meta is None:
        return None
    cenario = (meta.get("cenario") or "").upper()
    if cenario not in ("C6", "C7"):
        return None

    inicio_ms = float(meta.get("inicio_carga_ms", 0) or 0)
    fim_ms = float(meta.get("fim_carga_ms", 0) or 0)
    if not inicio_ms or fim_ms <= inicio_ms:
        return None
    inicio_s = int(inicio_ms / 1000.0)
    # A janela inclui a drenagem: em C6 e C7 o que interessa e justamente o que
    # acontece depois do evento.
    fim_s = int(fim_ms / 1000.0) + int(float(meta.get("drenagem_s", 0) or 0)) + 5

    concluidas, latencias = _serie_consumidor(pasta, inicio_s, fim_s)
    emitidas, aceitas, erros = _serie_produtor(pasta, inicio_s, fim_s)
    fila, nao_conf = _serie_fila(pasta, inicio_s, fim_s)

    linhas = []
    for segundo in range(inicio_s, fim_s + 1):
        lats = latencias.get(segundo, [])
        linhas.append({
            "segundo": segundo - inicio_s,
            "instante_ms": segundo * 1000,
            "fase": "",
            "emitidas_s": emitidas.get(segundo, 0),
            "aceitas_s": aceitas.get(segundo, 0),
            "erros_s": erros.get(segundo, 0),
            "vazao_s": concluidas.get(segundo, 0),
            "lat_p50_ms": round(statistics.median(lats), 3) if lats else "",
            "lat_max_ms": round(max(lats), 3) if lats else "",
            "fila_mensagens": fila.get(segundo, ""),
            "fila_nao_confirmadas": nao_conf.get(segundo, ""),
        })

    faixa = _fases_c7(meta, inicio_s) if cenario == "C7" else _fases_c6(linhas)
    if faixa:
        comeco, termino = faixa
        for linha in linhas:
            if linha["segundo"] < comeco:
                linha["fase"] = "antes"
            elif linha["segundo"] <= termino:
                linha["fase"] = "durante"
            else:
                linha["fase"] = "depois"

    destino = os.path.join(pasta, "eventos.csv")
    with open(destino, "w", encoding="utf-8", newline="") as f:
        escritor = csv.DictWriter(f, fieldnames=COLUNAS)
        escritor.writeheader()
        escritor.writerows(linhas)

    return {
        "pasta": pasta,
        "execucao_id": meta.get("execucao_id", os.path.basename(pasta)),
        "cenario": cenario,
        "modo": (meta.get("modo") or "").lower(),
        "tamanho": str(meta.get("tamanho_mensagem", "")),
        "linhas": linhas,
        "faixa": faixa,
        "destino": destino,
        "linhas_banco": meta.get("linhas_banco"),
        "emitidas_total": sum(emitidas.values()),
    }


def indicadores(serie):
    """Os tres indicadores da secao 3.5."""
    linhas = serie["linhas"]
    faixa = serie["faixa"]
    resultado = {
        "recusados_ou_perdidos": "",
        "latencia_maxima_ms": "",
        "recuperacao_s": "",
        "nao_gravados": "",
    }

    latencias = [l["lat_max_ms"] for l in linhas if l["lat_max_ms"] != ""]
    if latencias:
        resultado["latencia_maxima_ms"] = round(max(latencias), 3)

    # Pedidos emitidos que nao foram gravados no banco ao fim da execucao.
    if isinstance(serie["linhas_banco"], int) and serie["emitidas_total"]:
        resultado["nao_gravados"] = serie["emitidas_total"] - serie["linhas_banco"]

    if not faixa:
        return resultado
    comeco, termino = faixa

    # Vazao util (secao 3.5): na via sincrona, as respostas aceitas pelo
    # solicitante; na assincrona, as conclusoes registradas pelo consumidor. E a
    # mesma grandeza que a consolidacao e a rampa usam, de modo que o tempo de
    # recuperacao nao seja medido por uma vazao que inclui o que chegou tarde.
    campo = "aceitas_s" if serie["modo"] == "sincrono" else "vazao_s"


    # Recusados ou perdidos durante o evento: erros no ponto de emissao, mais o
    # que foi emitido e nunca concluido dentro da janela do evento.
    durante = [l for l in linhas if comeco <= l["segundo"] <= termino]
    erros = sum(l["erros_s"] for l in durante)
    resultado["recusados_ou_perdidos"] = erros

    # Tempo de recuperacao: a partir do fim do evento, o primeiro segundo em que
    # a vazao volta ao patamar anterior e a fila deixa de estar acima dele.
    antes = [l for l in linhas if l["segundo"] < comeco]
    if not antes:
        return resultado
    vazao_base = statistics.median([l[campo] for l in antes]) if antes else 0
    filas_antes = [l["fila_mensagens"] for l in antes if l["fila_mensagens"] != ""]
    fila_base = statistics.median(filas_antes) if filas_antes else 0

    depois = [l for l in linhas if l["segundo"] > termino]
    for linha in depois:
        vazao_ok = vazao_base <= 0 or linha[campo] >= vazao_base * 0.9
        fila_atual = linha["fila_mensagens"]
        fila_ok = fila_atual == "" or fila_atual <= max(fila_base, 0) + 10
        if vazao_ok and fila_ok:
            resultado["recuperacao_s"] = linha["segundo"] - termino
            break
    else:
        if depois:
            resultado["recuperacao_s"] = f">{depois[-1]['segundo'] - termino}"
    return resultado


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dir", default="resultados")
    p.add_argument("--execucao", action="append", default=[])
    args = p.parse_args()

    pastas = list(args.execucao)
    if not pastas:
        pastas = [
            os.path.join(args.dir, nome)
            for nome in sorted(os.listdir(args.dir))
            if os.path.isdir(os.path.join(args.dir, nome))
        ]

    resumo = []
    for pasta in pastas:
        serie = montar(pasta)
        if serie is None:
            continue
        ind = indicadores(serie)
        resumo.append({
            "execucao_id": serie["execucao_id"],
            "cenario": serie["cenario"],
            "modo": serie["modo"],
            "tamanho_mensagem": serie["tamanho"],
            "evento_inicio_s": serie["faixa"][0] if serie["faixa"] else "",
            "evento_fim_s": serie["faixa"][1] if serie["faixa"] else "",
            **ind,
        })
        print("")
        print(f"{serie['execucao_id']} ({serie['cenario']}, via {serie['modo']}) -> {serie['destino']}")
        if serie["faixa"]:
            print(f"  evento entre {serie['faixa'][0]} s e {serie['faixa'][1]} s da execucao")
        else:
            print("  evento nao localizado na serie")
        print(f"  recusados ou perdidos durante o evento: {ind['recusados_ou_perdidos']}")
        print(f"  latencia maxima: {ind['latencia_maxima_ms']} ms")
        print(f"  tempo de recuperacao: {ind['recuperacao_s']} s")
        print(f"  emitidos e nao gravados ao final: {ind['nao_gravados']}")

    if not resumo:
        print("nenhuma execucao de C6 ou C7 encontrada em " + args.dir)
        return 1

    alvo = os.path.join(args.dir, "eventos_resumo.csv")
    with open(alvo, "w", encoding="utf-8", newline="") as f:
        escritor = csv.DictWriter(f, fieldnames=list(resumo[0].keys()))
        escritor.writeheader()
        escritor.writerows(resumo)
    print("")
    print(str(len(resumo)) + " execucoes complementares -> " + alvo)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
