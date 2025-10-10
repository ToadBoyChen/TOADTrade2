FROM python:3.12-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y sqlite3

ENV PYTHONUNBUFFERED=1

CMD python TT2.py