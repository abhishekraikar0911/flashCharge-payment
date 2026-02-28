# Stage 1: Backend only
FROM python:3.10-slim

WORKDIR /app

# Satisfy the Python requirement for a frontend directory in main.py
RUN mkdir -p frontend/build && touch frontend/build/index.html

# Copy backend files
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir --upgrade -r requirements.txt

ENV WEBSERVER_HOST="0.0.0.0"
ENV WEBSERVER_PORT=9010
EXPOSE $WEBSERVER_PORT

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9010"]
