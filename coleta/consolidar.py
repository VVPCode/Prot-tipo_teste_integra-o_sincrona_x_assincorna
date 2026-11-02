"""Tratamento e analise dos dados (secao 3.5 do artigo).

Le os arquivos brutos de cada execucao em resultados/<execucao_id>/ e produz:
  resultados/consolidado.csv - uma linha por execucao, uma coluna por variavel
  resultados/resumo.csv      - mediana das repeticoes por via, cenario e tamanho

Por padrao consolida apenas as execucoes sem rotulo, isto e, as da medicao
final. As execucoes de verificacao e de calibracao ficam de fora, e sao
acessadas por --rotulo ou --rotulo-prefixo.

Uso: python coleta/consolidar.py [--dir resultados] [--rotulo piloto]
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import statistics

SERVICOS = ("produtor", "consumidor", "rabbitmq", "postgres", "carga")

COLUNAS = [
    "execucao_id", "modo", "cenario", "tamanho_mensagem", "repeticao",
    "carga_alvo_ops", "duracao_s",
    "concorrencia", "pool_banco", "tempo_limite_ms", "mensagem_persistente",
    "cpu_produtor_limite", "cpu_servico_limite", "cpu_broker_limite",
    "cpu_postgres_limite",
    "emitidas", "descartadas_gerador", "recusadas", "concluidas_sucesso",
    "expiradas", "invalidas", "duplicadas", "falhas_gravacao", "mortas_dlq",
    "nao_processadas", "apos_limite", "prop_apos_limite",
    "taxa_erro", "vazao_util", "vazao_janela", "vazao_total",
    "linhas_banco", "divergencia_banco", "drenagem_s",
    "lat_media_ms", "lat_p50_ms", "lat_p95_ms", "lat_p99_ms", "lat_max_ms",
    "lat_gerador_p50_ms", "lat_gerador_p95_ms", "atraso_t0_p50_ms",
    "tamanho_corpo_medio", "fila_max", "fila_media",
]
for _s in SERVICOS:
    COLUNAS += [f"cpu_{_s}_pct", f"cpu_{_s}_pico", f"mem_{_s}_mib", f"mem_{_s}_pico"]

DEPENDENTES = [
    "lat_media_ms", "lat_p50_ms", "lat_p95_ms", "lat_p99_ms", "lat_max_ms",
    "vazao_util", "vazao_janela", "vazao_total", "taxa_erro", "prop_apos_limite",
    "drenagem_s", "fila_max", "linhas_banco", "atraso_t0_p50_ms",
]
for _s in SERVICOS:
    DEPENDENTES += [f"cpu_{_s}_pct", f"cpu_{_s}_pico", f"mem_{_s}_mib", f"mem_{_s}_pico"]


def percentil(valores, p):
    """Percentil por posicao ordenada (metodo do rank mais proximo)."""
    if not valores:
        return float("nan")
    ordenados = sorted(valores)
    k = int(round(p / 100.0 * len(ordenados) + 0.5)) - 1
    k = max(0, min(len(ordenados) - 1, k))
    return ordenados[k]


def _media(valores):
    return statistics.fmean(valores) if valores else float("nan")


def _red(valor, casas=3):
    return round(valor, casas) if valor == valor else ""


def ler_consumidor(caminho, inicio_ms, fim_ms, tempo_limite_ms=0, aplicar_prazo=False):
    """Le o registro do consumidor: T1 das duas vias (Quadro 5).

    As conclusoes posteriores ao tempo limite sao contadas nas duas vias, porque
    a proporcao de operacoes assincronas concluidas depois do prazo e o
    contraponto direto da taxa de erro da via sincrona.

    O prazo so e descontado da vazao util na via sincrona: ali a conclusao
    tardia e trabalho perdido, pois o solicitante ja recebeu erro. Na via
    assincrona o solicitante aguarda apenas a aceitacao da mensagem, e a espera
    em fila e o comportamento esperado (secao 2.3).
    """
    vazio = {
        "latencias": [], "latencias_gerador": [], "atrasos_t0": [],
        "tamanhos": [], "contagens": {}, "na_janela": 0, "uteis_janela": 0,
        "apos_limite": 0, "ultimo_ms": 0.0,
    }
    contagens = {
        "sucesso": 0, "duplicada": 0, "invalida": 0,
        "falha_gravacao": 0, "expirada": 0,
    }
    vazio["contagens"] = contagens
    if not os.path.exists(caminho):
        return vazio

    latencias, latencias_gerador, atrasos, tamanhos = [], [], [], []
    na_janela = uteis_janela = apos_limite = 0
    ultimo_ms = 0.0
    with open(caminho, encoding="utf-8", newline="") as f:
        for linha in csv.DictReader(f):
            situacao = linha["situacao"]
            contagens[situacao] = contagens.get(situacao, 0) + 1
            if situacao != "sucesso":
                continue
            t1 = float(linha["t1_ms"])
            ultimo_ms = max(ultimo_ms, t1)
            lat = float(linha["latencia_ms"])
            lat_ger = float(linha.get("latencia_gerador_ms", -1) or -1)
            if lat >= 0:
                latencias.append(lat)
            if lat_ger >= 0:
                latencias_gerador.append(lat_ger)
                if lat >= 0:
                    # Espera entre a emissao pelo gerador e T0, isto e, o que a
                    # fila de conexoes do produtor adiciona antes da medicao.
                    atrasos.append(lat_ger - lat)
            tamanhos.append(int(linha.get("tamanho_corpo", 0) or 0))

            dentro_do_prazo = tempo_limite_ms <= 0 or lat <= tempo_limite_ms
            if not dentro_do_prazo:
                apos_limite += 1
            if inicio_ms <= t1 <= fim_ms:
                na_janela += 1
                if dentro_do_prazo or not aplicar_prazo:
                    uteis_janela += 1

    return {
        "latencias": latencias,
        "latencias_gerador": latencias_gerador,
        "atrasos_t0": atrasos,
        "tamanhos": tamanhos,
        "contagens": contagens,
        "na_janela": na_janela,
        "uteis_janela": uteis_janela,
        "apos_limite": apos_limite,
        "ultimo_ms": ultimo_ms,
    }


def ler_k6(caminho):
    """Le o resumo do gerador: pedidos emitidos, aceitos, recusados e descartados."""
    if not os.path.exists(caminho):
        return {}
    with open(caminho, encoding="utf-8") as f:
        dados = json.load(f)
    metricas = dados.get("resumo", {}).get("metrics", {})

    def contador(nome):
        valor = metricas.get(nome, {}).get("values", {}).get("count")
        return int(valor) if valor is not None else 0

    return {
        # Demanda que o gerador nao conseguiu emitir. Nao e erro do servico: se
        # for relevante, o proprio gerador foi o componente saturado, o que e
        # ameaca a validade interna discutida na secao 3.6.
        "descartadas_gerador": contador("dropped_iterations"),
        "emitidas": contador("exp_emitidas"),
        "aceitas": contador("exp_aceitas"),
        "erros_http": contador("exp_erros_http"),
        "erros_tempo_limite": contador("exp_erros_tempo_limite"),
    }


def ler_recursos(caminho, inicio_ms=0.0, fim_ms=0.0):
    """Medias e picos de CPU e memoria restritos a janela de carga.

    Fora da janela estao o arranque dos conteineres e a drenagem da fila, que
    rebaixariam a media sem corresponder ao regime medido. O conteiner do
    gerador entra como "carga", para que se possa verificar se ele proprio
    saturou, e o do PostgreSQL porque banco saturado esconde a diferenca entre
    os estilos (secao 3.6).
    """
    cpu, mem = {}, {}
    if not os.path.exists(caminho):
        return {"cpu": cpu, "mem": mem}
    with open(caminho, encoding="utf-8", newline="") as f:
        for linha in csv.DictReader(f):
            if inicio_ms and fim_ms:
                instante = float(linha["instante_ms"])
                if not (inicio_ms <= instante <= fim_ms):
                    continue
            nome = linha["conteiner"]
            for servico in SERVICOS:
                if servico not in nome:
                    continue
                valor_cpu = float(linha["cpu_pct"])
                valor_mem = float(linha["mem_mib"])
                if valor_cpu >= 0:
                    cpu.setdefault(servico, []).append(valor_cpu)
                if valor_mem >= 0:
                    mem.setdefault(servico, []).append(valor_mem)
    return {"cpu": cpu, "mem": mem}


def ler_fila(caminho):
    prontas, mortas = [], 0
    if not os.path.exists(caminho):
        return {"prontas": prontas, "mortas": 0}
    with open(caminho, encoding="utf-8", newline="") as f:
        for linha in csv.DictReader(f):
            if linha["fila"] == "pedidos.novos":
                prontas.append(float(linha["mensagens"]))
            elif linha["fila"] == "pedidos.mortas":
                mortas = max(mortas, int(linha["mensagens"]))
    return {"prontas": prontas, "mortas": mortas}


def consolidar_execucao(pasta, rotulo="", prefixo=None):
    meta_caminho = os.path.join(pasta, "metadados.json")
    if not os.path.exists(meta_caminho):
        return None
    with open(meta_caminho, encoding="utf-8") as f:
        meta = json.load(f)

    marca = meta.get("rotulo") or ""
    if prefixo is not None:
        if not marca.startswith(prefixo):
            return None
    elif marca != rotulo:
        return None

    conf = meta.get("configuracao") or {}
    if not isinstance(conf, dict):
        conf = dict(conf)

    inicio_ms = float(meta.get("inicio_carga_ms", 0) or 0)
    fim_ms = float(meta.get("fim_carga_ms", 0) or 0)
    duracao_s = max((fim_ms - inicio_ms) / 1000.0, 1e-9)
    modo = (meta.get("modo") or "").lower()

    try:
        limite_ms = float(conf.get("TEMPO_LIMITE_MS", 0) or 0)
    except (TypeError, ValueError):
        limite_ms = 0.0

    cons = ler_consumidor(
        os.path.join(pasta, "consumidor.csv"), inicio_ms, fim_ms, limite_ms,
        aplicar_prazo=(modo == "sincrono"),
    )
    k6 = ler_k6(os.path.join(pasta, "k6_resumo.json"))
    rec = ler_recursos(os.path.join(pasta, "recursos.csv"), inicio_ms, fim_ms)
    fila = ler_fila(os.path.join(pasta, "fila.csv"))

    sucessos = cons["contagens"].get("sucesso", 0)
    emitidas = k6.get("emitidas", 0)

    # Taxa de erro: proporcao de pedidos emitidos sem registro T1 de sucesso -
    # erro HTTP, tempo esgotado, falha de validacao ou de gravacao e mensagens
    # nao processadas ao fim da janela de drenagem (Quadro 5). E medida no ponto
    # de emissao: um pedido cujo solicitante recebeu erro por tempo esgotado nao
    # se torna concluido porque o consumidor o terminou depois.
    recusadas = k6.get("erros_http", 0) + k6.get("erros_tempo_limite", 0)
    mortas = fila["mortas"]
    nao_processadas = max(0, emitidas - recusadas - sucessos - mortas)
    nao_concluidas = recusadas + mortas + nao_processadas
    taxa_erro = nao_concluidas / emitidas if emitidas else float("nan")

    # Vazao util: pedidos concluidos de modo a servir a quem os pediu. Na via
    # sincrona sao as respostas que o solicitante aceitou; contar no consumidor
    # as conclusoes dentro do prazo superestimaria a via, porque a operacao pode
    # terminar no prazo pelo relogio do consumidor e a resposta chegar depois.
    uteis = k6.get("aceitas", 0) if modo == "sincrono" else cons["uteis_janela"]

    linhas_banco = meta.get("linhas_banco")
    # Conferencia da vazao: as linhas gravadas devem igualar os registros T1 de
    # sucesso somados as reentregas, que nao produzem segunda linha.
    divergencia = ""
    if isinstance(linhas_banco, int):
        divergencia = linhas_banco - sucessos

    drenagem = max(0.0, (cons["ultimo_ms"] - fim_ms) / 1000.0) if cons["ultimo_ms"] else 0.0

    registro = {
        "execucao_id": meta.get("execucao_id", os.path.basename(pasta)),
        "modo": modo,
        "cenario": meta.get("cenario", ""),
        "tamanho_mensagem": meta.get("tamanho_mensagem", ""),
        "repeticao": meta.get("repeticao", 0),
        "carga_alvo_ops": meta.get("carga_alvo_ops", ""),
        "duracao_s": round(duracao_s, 3),
        "concorrencia": conf.get("CONCORRENCIA", ""),
        "pool_banco": conf.get("POOL_BANCO", ""),
        "tempo_limite_ms": conf.get("TEMPO_LIMITE_MS", ""),
        "mensagem_persistente": conf.get("MENSAGEM_PERSISTENTE", ""),
        "cpu_produtor_limite": conf.get("CPU_PRODUTOR", ""),
        "cpu_servico_limite": conf.get("CPU_SERVICO", ""),
        "cpu_broker_limite": conf.get("CPU_BROKER", ""),
        "cpu_postgres_limite": conf.get("CPU_POSTGRES", ""),
        "emitidas": emitidas,
        "descartadas_gerador": k6.get("descartadas_gerador", 0),
        "recusadas": recusadas,
        "concluidas_sucesso": sucessos,
        "expiradas": cons["contagens"].get("expirada", 0),
        "invalidas": cons["contagens"].get("invalida", 0),
        "duplicadas": cons["contagens"].get("duplicada", 0),
        "falhas_gravacao": cons["contagens"].get("falha_gravacao", 0),
        "mortas_dlq": mortas,
        "nao_processadas": nao_processadas,
        "apos_limite": cons["apos_limite"],
        "prop_apos_limite": round(cons["apos_limite"] / sucessos, 4) if sucessos else "",
        "taxa_erro": _red(taxa_erro, 6),
        "vazao_util": round(uteis / duracao_s, 3),
        "vazao_janela": round(cons["na_janela"] / duracao_s, 3),
        "vazao_total": round(sucessos / duracao_s, 3),
        "linhas_banco": linhas_banco if linhas_banco is not None else "",
        "divergencia_banco": divergencia,
        "drenagem_s": round(drenagem, 3),
        "lat_media_ms": _red(_media(cons["latencias"])),
        "lat_p50_ms": _red(percentil(cons["latencias"], 50)),
        "lat_p95_ms": _red(percentil(cons["latencias"], 95)),
        "lat_p99_ms": _red(percentil(cons["latencias"], 99)),
        "lat_max_ms": round(max(cons["latencias"]), 3) if cons["latencias"] else "",
        "lat_gerador_p50_ms": _red(percentil(cons["latencias_gerador"], 50)),
        "lat_gerador_p95_ms": _red(percentil(cons["latencias_gerador"], 95)),
        # Quanto a espera antes de T0 subtrai da latencia medida. Cresce quando o
        # produtor satura, e e a conferencia prevista no planejamento.
        "atraso_t0_p50_ms": _red(percentil(cons["atrasos_t0"], 50)),
        "tamanho_corpo_medio": _red(_media(cons["tamanhos"]), 1),
        "fila_max": int(max(fila["prontas"])) if fila["prontas"] else 0,
        "fila_media": _red(_media(fila["prontas"]), 2) if fila["prontas"] else 0,
    }
    for servico in SERVICOS:
        valores_cpu = rec["cpu"].get(servico, [])
        valores_mem = rec["mem"].get(servico, [])
        registro[f"cpu_{servico}_pct"] = _red(_media(valores_cpu), 2)
        registro[f"cpu_{servico}_pico"] = round(max(valores_cpu), 2) if valores_cpu else ""
        registro[f"mem_{servico}_mib"] = _red(_media(valores_mem), 2)
        registro[f"mem_{servico}_pico"] = round(max(valores_mem), 2) if valores_mem else ""
    return registro


def resumir(linhas):
    """Mediana das repeticoes por via, cenario e tamanho (item c do protocolo)."""
    grupos = {}
    for linha in linhas:
        chave = (linha["modo"], linha["cenario"], str(linha["tamanho_mensagem"]))
        grupos.setdefault(chave, []).append(linha)
    resumo = []
    for chave in sorted(grupos, key=lambda c: (c[1], c[2], c[0])):
        itens = grupos[chave]
        registro = {
            "modo": chave[0],
            "cenario": chave[1],
            "tamanho_mensagem": chave[2],
            "carga_alvo_ops": itens[0]["carga_alvo_ops"],
            "repeticoes": len(itens),
        }
        for coluna in DEPENDENTES:
            valores = []
            for item in itens:
                valor = item[coluna]
                if valor == "" or valor is None:
                    continue
                valor = float(valor)
                if valor == valor:
                    valores.append(valor)
            registro[coluna + "_mediana"] = round(statistics.median(valores), 3) if valores else ""
            # Dispersao observada, relatada em lugar do desvio padrao dado o
            # numero reduzido de repeticoes (secao 3.6).
            registro[coluna + "_amplitude"] = round(max(valores) - min(valores), 3) if len(valores) > 1 else 0
        resumo.append(registro)
    return resumo


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dir", default="resultados")
    p.add_argument("--rotulo", default="", help="rotulo das execucoes a consolidar")
    p.add_argument("--rotulo-prefixo", default=None,
                   help="consolida todas as execucoes cujo rotulo comece por este texto")
    args = p.parse_args()

    linhas = []
    for nome in sorted(os.listdir(args.dir)):
        pasta = os.path.join(args.dir, nome)
        if not os.path.isdir(pasta):
            continue
        registro = consolidar_execucao(pasta, args.rotulo, args.rotulo_prefixo)
        if registro:
            linhas.append(registro)

    if not linhas:
        if args.rotulo_prefixo is not None:
            alvo_texto = "com rotulo iniciado por " + args.rotulo_prefixo
        else:
            alvo_texto = "sem rotulo" if args.rotulo == "" else "com rotulo " + args.rotulo
        print("nenhuma execucao " + alvo_texto + " encontrada em " + args.dir)
        return 1

    if args.rotulo_prefixo is not None:
        complemento = "_" + args.rotulo_prefixo.rstrip("_")
    else:
        complemento = "" if args.rotulo == "" else "_" + args.rotulo

    alvo = os.path.join(args.dir, "consolidado" + complemento + ".csv")
    with open(alvo, "w", encoding="utf-8", newline="") as f:
        escritor = csv.DictWriter(f, fieldnames=COLUNAS)
        escritor.writeheader()
        escritor.writerows(linhas)

    resumo = resumir(linhas)
    alvo_resumo = os.path.join(args.dir, "resumo" + complemento + ".csv")
    with open(alvo_resumo, "w", encoding="utf-8", newline="") as f:
        escritor = csv.DictWriter(f, fieldnames=list(resumo[0].keys()))
        escritor.writeheader()
        escritor.writerows(resumo)

    print(str(len(linhas)) + " execucoes consolidadas -> " + alvo)
    print(str(len(resumo)) + " combinacoes via x cenario x tamanho -> " + alvo_resumo)

    divergentes = [l for l in linhas if l["divergencia_banco"] not in ("", 0)]
    if divergentes:
        print("")
        print("ATENCAO: linhas no banco diferentes dos registros T1 de sucesso:")
        for l in divergentes:
            print(f"  {l['execucao_id']}: banco {l['linhas_banco']}, "
                  f"T1 {l['concluidas_sucesso']}, diferenca {l['divergencia_banco']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
