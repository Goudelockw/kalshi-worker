FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONUNBUFFERED=1
# Default: always-on snapshot worker. Other Railway services override the
# start command, e.g. `python -m kalshi_worker sync` or `... reconcile`.
CMD ["python", "-m", "kalshi_worker", "worker"]
