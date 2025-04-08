FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir .

# Copy source code
COPY main.py ./

# Create a non-root user to run the application
RUN useradd -m appuser
USER appuser

# Environment variables will be provided at runtime
ENV DB_NAME=state_aid_db \
    DB_USER=postgres \
    DB_PASSWORD=postgres \
    DB_HOST=db \
    DB_PORT=5432

# Run the scraper
CMD ["python", "main.py", "run"]