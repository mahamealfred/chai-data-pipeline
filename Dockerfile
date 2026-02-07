FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY scripts/ ./scripts/
COPY orchestration/ ./orchestration/
COPY config/ ./config/
COPY tests/ ./tests/

# Create architecture directories
RUN mkdir -p /app/data/bronze /app/data/silver /app/data/gold

# Create entrypoint script
RUN echo '#!/bin/bash\npython orchestration/chai_data_pipeline.py' > /app/entrypoint.sh \
    && chmod +x /app/entrypoint.sh

CMD ["python", "orchestration/chai_data_pipeline.py"]