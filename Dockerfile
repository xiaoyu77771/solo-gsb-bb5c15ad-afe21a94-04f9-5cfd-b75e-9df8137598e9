FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HOST=0.0.0.0 \
    PORT=8080

WORKDIR /srv

# 先装依赖，利用构建缓存
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 再拷应用代码
COPY app ./app

# 非 root 运行
RUN useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser /srv
USER appuser

EXPOSE 8080

# 容器内健康检查（标准库，无需额外依赖）
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import os,urllib.request; \
urllib.request.urlopen('http://127.0.0.1:%s/healthz' % os.environ.get('PORT','8080'), timeout=2).read()" \
    || exit 1

# 依赖在镜像构建时安装，应用在 uvicorn 导入时完成初始化；单 worker（内存状态）
CMD ["sh", "-c", "uvicorn app.main:app --host ${HOST:-0.0.0.0} --port ${PORT:-8080}"]
