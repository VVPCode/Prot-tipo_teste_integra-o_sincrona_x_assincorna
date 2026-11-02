FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY services/produtor/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY comum ./comum
COPY services/produtor/app.py ./app.py

EXPOSE 8000
# Ver a observacao sobre "exec" no Dockerfile do consumidor.
CMD ["sh", "-c", "exec uvicorn app:app --host 0.0.0.0 --port 8000 --workers ${WORKERS:-1} --backlog ${BACKLOG:-2048} --log-level warning --timeout-graceful-shutdown ${ENCERRAMENTO_S:-5}"]
