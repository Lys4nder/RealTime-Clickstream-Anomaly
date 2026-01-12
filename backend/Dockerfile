# backend/Dockerfile
FROM python:3.12-slim

# 1. Install uv (The magic part)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# 2. Copy the lockfiles first (for caching)
# uv init created these for you
COPY pyproject.toml uv.lock ./

# 3. Install dependencies using uv
# Configure uv to use the system python by default
ENV UV_PROJECT_ENVIRONMENT="/usr/local"

# Now run sync (it will auto-detect the system env via the variable above)
RUN uv sync --frozen

# 4. Copy the rest of the code
COPY . .

# 5. Run it
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]