# pipelines/Dockerfile
FROM apache/spark:3.5.0

USER root

WORKDIR /app

# 1. Copy ALL files needed for the package build first
# We need pyproject.toml, README.md, and the src/ folder
COPY pyproject.toml README.md ./
COPY src/ ./src/

# 2. Install
# Now 'pip install .' will find the src folder and README and succeed
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --system .

# 3. Set Python Path
# Since we installed the package with pip, this is technically optional now, 
# but keeping it ensures there are no import ambiguities.
ENV PYTHONPATH="/app/src:${PYTHONPATH}"
