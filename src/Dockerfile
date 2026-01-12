FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

# Instalación de dependencias del sistema y Google Chrome
# (COPIADO DE TU DOCKERFILE PROBADO)
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    apt-transport-https \
    unzip \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome-main.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-main.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y \
    google-chrome-stable \
    libgbm1 \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copiar requerimientos e instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar los scripts
COPY main.py .
COPY etl_seia.py .

# EJECUCIÓN DIRECTA (Cloud Run Job)
CMD ["python", "main.py"]