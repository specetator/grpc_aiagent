FROM python:3.12-slim-bookworm AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    HF_HUB_OFFLINE=1 HF_HUB_DISABLE_TELEMETRY=1 OMP_NUM_THREADS=2
RUN apt-get update && apt-get install -y --no-install-recommends \
      ffmpeg poppler-utils tesseract-ocr tesseract-ocr-eng tesseract-ocr-chi-sim \
      fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /opt/spark-media/requirements.txt
RUN pip install --no-cache-dir -r /opt/spark-media/requirements.txt
COPY parser.py /opt/spark-media/parser.py
USER 65534:65534
ENTRYPOINT ["python3", "/opt/spark-media/parser.py"]

# Fixture generator only; the deployed parser image stops at --target runtime.
FROM runtime AS fixtures
USER root
RUN apt-get update && apt-get install -y --no-install-recommends espeak-ng \
    && rm -rf /var/lib/apt/lists/*
USER 65534:65534
