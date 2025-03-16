FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Asegurarte que gunicorn está en requirements.txt
# Definir explícitamente PORT como variable de entorno
ENV PORT=8080

# Comando más explícito
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app:app 