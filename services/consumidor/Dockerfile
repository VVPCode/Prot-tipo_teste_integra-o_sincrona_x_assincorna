FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY services/consumidor/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY comum ./comum
COPY services/consumidor/app.py ./app.py

EXPOSE 8001
# "exec" e indispensavel: sem ele o sh permanece como processo 1 e nao
# repassa o SIGTERM, de modo que "docker compose stop" termina em SIGKILL e o
# encerramento do servico nao ocorre. No cenario C7, em que o conteiner e
# interrompido de proposito, isso descartava os registros T1 ainda em memoria
# e as linhas gravadas no banco deixavam de conferir com eles.
# O limite de encerramento suave garante a saida dentro da carencia do Docker
# mesmo com conexoes abertas pelo produtor.
CMD ["sh", "-c", "exec uvicorn app:app --host 0.0.0.0 --port 8001 --workers 1 --backlog ${BACKLOG:-2048} --log-level warning --timeout-graceful-shutdown ${ENCERRAMENTO_S:-5}"]
