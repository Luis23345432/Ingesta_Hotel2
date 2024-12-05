FROM python:3.8-slim

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

# Ensure the logs directory exists
RUN mkdir -p /app/logs

# Set the entrypoint to dynamically receive the script name
ENTRYPOINT ["python"]
CMD ["ingesta4.py"]
