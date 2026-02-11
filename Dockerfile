# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY pyproject.toml README.md ./

RUN --mount=type=cache,target=/root/.cache/pip \
    python -c "import pathlib,tomllib; deps=tomllib.loads(pathlib.Path('pyproject.toml').read_text())['project']['dependencies']; pathlib.Path('requirements.txt').write_text('\n'.join(deps)+'\n')" \
    && pip install --upgrade pip \
    && pip install --no-compile -r requirements.txt \
    && rm -f requirements.txt

COPY src ./src

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-deps --no-compile .

EXPOSE 8080

CMD ["uvicorn", "simpl_bulk_dataplane.main:app", "--host", "0.0.0.0", "--port", "8080"]
