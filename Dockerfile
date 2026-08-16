FROM python:3.14-slim
WORKDIR /app
RUN useradd --create-home --uid 1000 raft \
    && mkdir -p /data /var/log/raftkv \
    && chown -R raft /data /var/log/raftkv
# Hash-checked install from the exported lock: image builds cannot float transitives.
COPY requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir --no-deps .
USER raft
ENV RAFT_DB_PATH=/data/raft.db RAFT_LOG_DIR=/var/log/raftkv
EXPOSE 8000
CMD ["uvicorn", "--factory", "raftkv.app:create_app", "--host", "0.0.0.0", "--port", "8000"]
