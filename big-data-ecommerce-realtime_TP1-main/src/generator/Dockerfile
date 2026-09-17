FROM python:3.12-slim

WORKDIR /app

COPY gerador.py /app/gerador.py

CMD ["python", "/app/gerador.py", "--output", "/var/log/myapp/events.log", "--interval", "0.5"]