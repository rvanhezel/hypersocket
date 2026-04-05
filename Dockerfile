FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt python-logging-loki
COPY . .
CMD ["python", "perp_quoter.py"]
