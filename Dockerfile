FROM python:3.11-slim
WORKDIR /app

# Dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Instalar poetry
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    export PATH="/root/.local/bin:$PATH"

ENV PATH="/root/.local/bin:$PATH"

# Configurar Poetry para crear virtualenv en el proyecto
RUN poetry config virtualenvs.in-project true

# Copiar archivos de configuración
COPY pyproject.toml poetry.lock* /app/

# Instalar dependencias primero (sin código fuente)
RUN poetry install --only main --no-root --no-interaction --no-ansi

# Ahora copiar el código fuente
COPY . /app/

# Usar el virtualenv directamente en lugar de poetry run
CMD [".venv/bin/uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8080"]
