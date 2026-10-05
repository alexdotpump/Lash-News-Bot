FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./

RUN python -m pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "main.py"]
