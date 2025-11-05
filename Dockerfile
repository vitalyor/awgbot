FROM python:3.12-alpine

# Базовые утилиты (docker-cli для работы через docker-proxy), часовой пояс и сертификаты
RUN apk add --no-cache docker-cli jq tzdata ca-certificates bash \
    && update-ca-certificates

WORKDIR /app

# Установка зависимостей
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Код бота
COPY src/ /app/

# Каталог для состояния/данных
RUN mkdir -p /app/data

# Создаём фиксированного непривилегированного пользователя (UID/GID 10001) и настраиваем права
RUN addgroup -g 10001 app \
    && adduser -D -h /app -G app -u 10001 app \
    && chown -R app:app /app

USER app

# Удобные дефолты для Python (без буферизации и .pyc в контейнере)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UMASK=027

CMD ["python", "-u", "bot.py"]