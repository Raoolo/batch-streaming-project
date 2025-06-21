FROM python:3.13.5-slim
WORKDIR /app    
COPY stream_simulator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY stream_simulator/ ./stream_simulator/
# 6. Set environment variables for container, this makes sure that Python output is sent directly to terminal without buffering
ENV PYTHONUNBUFFERED=1
CMD ["python", "stream_simulator/stream_to_s3.py"]