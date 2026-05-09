FROM mcr.microsoft.com/playwright/python:v1.49.0-jammy

WORKDIR /app

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y tzdata && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Kolkata

CMD ["python", "-u", "scheduler.py"]
