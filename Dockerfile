FROM python:3.12-slim

# ✅ Prevent Python from writing .pyc files & buffer output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# ✅ Install system dependencies (minimal)
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        tzdata \
        tini \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# ✅ Install Python dependencies first (cache layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ✅ Copy application code
COPY bot.py pomodoro.py weekly_report.py ./

# ✅ Create non-root user
RUN useradd --create-home --shell /usr/sbin/nologin appuser \
    && mkdir -p /data/backups \
    && chown -R appuser:appuser /app /data

# ✅ Switch to non-root user
USER appuser

# ✅ Set working directory to data dir
WORKDIR /data

# ✅ Health check script
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c 'import os; exit(0 if os.path.exists("/data/study_data.json") else 1)'

# ✅ Use tini for proper signal handling
ENTRYPOINT ["/usr/bin/tini", "--"]

# ✅ Run bot
CMD ["python", "/app/bot.py"]