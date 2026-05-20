# ---- Stage 1: Build the frontend ----
# Defaults point at Microsoft Container Registry so network-isolated ACR Tasks
# builds use the AI Landing Zone firewall allow-list without Docker Hub egress.
ARG REGISTRY=mcr.microsoft.com
ARG NODE_IMAGE=devcontainers/javascript-node:20
ARG PYTHON_IMAGE=devcontainers/python:3.12-bookworm
FROM ${REGISTRY}/${NODE_IMAGE} AS frontend-build
WORKDIR /build
COPY frontend/package*.json ./frontend/
RUN cd frontend && npm ci
COPY frontend/ ./frontend/
RUN cd frontend && npm run build
# Output is at /build/static (vite outDir: '../static')

# ---- Stage 2: Python application ----
FROM ${REGISTRY}/${PYTHON_IMAGE}

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

COPY . .

# Copy the built frontend into /app/static
COPY --from=frontend-build /build/static ./static

# Use a non-privileged port by default; the Container App ingress targetPort should match.
EXPOSE 8080

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}"]
