FROM python:3.11-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# source: 8080；cache: 8081
EXPOSE 8080 8081

CMD ["uvicorn", "--help"]
