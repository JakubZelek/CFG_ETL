FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    clang-11 clang++-11 \
    cmake \
    git \
    curl \
    libcurl4-openssl-dev \
    build-essential \
    pkg-config \
    libsqlite3-dev \
    libboost-all-dev \
    libleptonica-dev \
    libssl-dev \
    libprotobuf-dev protobuf-compiler \
    libevent-dev \
    libglib2.0-dev \
    && rm -rf /var/lib/apt/lists/*

ENV CC=clang-11
ENV CXX=clang++-11

WORKDIR /app

COPY generate_raw_cfg.py accumulate_repos.py ./

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "accumulate_repos.py"]
