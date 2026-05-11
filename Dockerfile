Copy

FROM python:3.12-slim
 
WORKDIR /app
 
# Install pip dependencies first (cached layer — only rebuilds if requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
 
# Install Chromium + all its system dependencies in one step
RUN playwright install --with-deps chromium
 
# Copy the rest of the codebase
COPY . .
 
CMD ["python", "main_scheduler.py"]