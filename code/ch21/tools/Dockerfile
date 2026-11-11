# Python with the Kafka client: generates the orders and shows the results
FROM python:3.12-slim
RUN pip install --no-cache-dir confluent-kafka==2.16.0
COPY tools.py /tools.py
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/tools.py"]
