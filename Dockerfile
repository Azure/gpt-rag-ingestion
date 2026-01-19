FROM mcr.microsoft.com/devcontainers/python:dev-3.12

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

COPY . .

# Use a non-privileged port by default; the Container App ingress targetPort should match.
EXPOSE 8080

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}"]