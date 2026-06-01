FROM python:3.10-slim

WORKDIR /app

# Install build deps for any wheels that need compiling (pandas/numpy etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy the package source and install morphic-util plus the service extras.
# Build context for this Dockerfile is the repository root, e.g.:
#   docker build -f service/Dockerfile -t morphic-util-service .
COPY . /app

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir ".[service]"

EXPOSE 8000

CMD ["uvicorn", "service.app:app", "--host", "0.0.0.0", "--port", "8000"]
