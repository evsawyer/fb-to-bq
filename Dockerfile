FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy only the requirements file first
COPY pyproject.toml .
COPY uv.lock .

RUN uv sync --locked

# Copy only the necessary application files
COPY main.py .
COPY bigquery.py .
COPY facebook.py .
COPY validate.py .
COPY schema.py .
COPY kpi_event_mapping_table.py .
COPY rollup.py .

# Set the PORT environment variable to 8080
ENV PORT=8080

# Expose the port
EXPOSE 8080

# Run the application with uvicorn
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]