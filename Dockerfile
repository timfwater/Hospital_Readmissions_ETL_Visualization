FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy EVERYTHING needed by the pipeline (scripts, streamlit, sql, etc.)
COPY . .

# Default: run the pipeline
CMD ["python", "run_full_pipeline.py"]
