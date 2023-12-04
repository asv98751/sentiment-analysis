FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

RUN python -m pip install "pymongo[srv]"

CMD ["python", "sentiment_consumer.py"]