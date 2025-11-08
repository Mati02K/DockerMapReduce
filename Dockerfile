FROM python:3.10-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

# copy deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy app
COPY coordinator.py worker.py ./

# since docker-compose just printing here
CMD ["python", "-c", "print('set by docker-compose')"]
