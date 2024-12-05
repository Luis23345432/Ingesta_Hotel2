FROM python:3.8-slim

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

RUN mkdir -p /logs

ENTRYPOINT ["python", "./ingesta2.py"]
