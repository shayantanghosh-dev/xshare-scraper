FROM mcr.microsoft.com/playwright/python:v1.49.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "-u", "scheduler.py"]
