FROM mcr.microsoft.com/playwright/python:v1.49.0-noble
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN chmod +x /app/entrypoint.sh
ENV PORT=8080 \
    PYTHONUNBUFFERED=1
EXPOSE 8080
CMD ["/app/entrypoint.sh"]
