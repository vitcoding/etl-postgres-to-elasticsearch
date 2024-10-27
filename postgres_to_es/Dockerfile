FROM python:3.11



WORKDIR /opt/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt .

RUN apt-get update \
    # && apt-get -y install libpq-dev gcc \
    # && apt-get install -y build-essential python3-dev libpq-dev \
    && apt-get install -y netcat-openbsd \
    && pip install --upgrade --no-cache-dir pip \
    # && pip install psycopg2 \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["sh", "/opt/app/entrypoint.sh"]