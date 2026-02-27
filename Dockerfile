FROM python:3.11-slim

LABEL authors="Andrea Morales Vega"

WORKDIR /app

# Instalar dependencias del sistema necesarias
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements primero (mejora cache)
COPY requirements.txt .

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo el proyecto
COPY . .

# Exponer puerto Streamlit
EXPOSE 8501

# Ejecutar Streamlit
CMD ["streamlit", "run", "src/App/Culturaltrip.py", "--server.port=8501", "--server.address=0.0.0.0"]