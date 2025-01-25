# Базовый образ Python
FROM python:3.10-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем все файлы проекта
COPY . .

# Устанавливаем зависимости (если нужны)
RUN pip install --no-cache-dir asyncio

# Переменная окружения для выбора сервиса
ENV SERVICE=public_server

# Запускаем нужный сервис
CMD ["sh", "-c", "python -u /app/${SERVICE}.py"]