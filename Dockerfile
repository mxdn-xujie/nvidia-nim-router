# ---------- 构建阶段：安装依赖到 venv ----------
FROM python:3.11-slim AS builder
WORKDIR /build
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------- 运行阶段：仅拷贝 venv + app + web ----------
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    DATA_DIR=/data
RUN useradd -m -u 1000 router && mkdir -p /data && chown -R router:router /data
COPY --from=builder /opt/venv /opt/venv
WORKDIR /app
COPY app ./app
COPY web ./web
USER router
EXPOSE 8000
VOLUME /data
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/api/health', timeout=4).status == 200 else 1)"
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]