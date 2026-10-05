# Build stage: install the package and its pinned dependencies.
FROM python:3.12-slim AS build
WORKDIR /src
COPY pyproject.toml constraints.txt README.md ./
COPY src ./src
RUN pip install --no-cache-dir --prefix=/install . -c constraints.txt

# Runtime stage: no compilers, no source tree, non-root user.
FROM python:3.12-slim
RUN useradd --create-home --uid 10001 app
WORKDIR /app
COPY --from=build /install /usr/local
COPY alembic.ini ./
COPY migrations ./migrations

USER app
ENV PYTHONUNBUFFERED=1 \
    DUNNING_PORT=8000
EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=20s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/live')"

CMD ["python", "-m", "dunning"]
