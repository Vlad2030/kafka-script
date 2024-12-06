FROM python:3.12-slim

WORKDIR /app

COPY main.py /app/main.py
COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python3", "main.py"]
