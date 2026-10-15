# Imagen del owner caliente de tessera-db (M8, nivel 2).
#
# Un contenedor = UN tenant: el proceso adquiere el lease al arrancar, sirve
# POST /write|/read por HTTP desde SQLite local caliente y publica deltas por
# lote. Configuración 100% por variables de entorno — ver
# examples/owner-taskdef.json y la sección "Owner caliente (nivel 2)" del
# README de examples/.
#
# Contexto de build: la RAÍZ del repo (el COPY baja tessera/):
#   docker buildx build --platform linux/arm64 -f examples/owner.Dockerfile .
FROM public.ecr.aws/docker/library/python:3.12-slim

LABEL org.opencontainers.image.source=https://github.com/korz-sh/tessera-db
LABEL org.opencontainers.image.description="tessera-db hot owner (one tenant, warm SQLite over HTTP, batched publish)"

WORKDIR /app

# Solo deps de runtime (boto3 para el backend S3+DynamoDB, apsw para la ruta
# VFS del Store/compactor). Sin dev deps: la imagen no lleva tests ni CLI.
COPY tessera/ ./tessera/
RUN pip install --no-cache-dir boto3 apsw

# Defaults del contenedor: bind en todas las interfaces para que la ENI de
# awsvpc sea alcanzable, y logs sin buffer para awslogs.
ENV TESSERA_HOST=0.0.0.0 \
    TESSERA_PORT=8080 \
    PYTHONUNBUFFERED=1

EXPOSE 8080

# Liveness del puerto HTTP contra /health (el endpoint responde 200 aunque el
# owner esté fenced: es una sonda de proceso, no de fencing). python:3.12-slim
# no trae curl — urllib basta y no agrega dependencias.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=4)"]

# Non-root por uid numérico (sin entrada en /etc/passwd, válido en slim y
# amigable con OpenShift). El cache local hidratado va a /tmp/tessera-owner.
USER 1000

CMD ["python", "-m", "tessera.owner"]
