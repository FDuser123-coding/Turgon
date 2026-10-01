# turgon-splink: the Splink service for resolve steps with strategy: splink.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-compile -r requirements.txt
COPY turgon_splink.py .

# DuckDB writes its temporary files to the home directory.
RUN useradd --uid 65532 --create-home --home-dir /home/splink splink
USER 65532:65532
ENV HOME=/home/splink

EXPOSE 8080
ENTRYPOINT ["python", "/app/turgon_splink.py"]
