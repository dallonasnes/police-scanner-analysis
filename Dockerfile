FROM python:3.5

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y \
    build-essential \
    ca-certificates \
    gcc \
    git \
    libpq-dev \
    make \
    pkg-config \
    ffmpeg

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt