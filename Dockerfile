FROM ubuntu:24.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    libpq-dev \
    postgresql-client \
    software-properties-common \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install Python 3.14 from deadsnakes PPA
RUN add-apt-repository ppa:deadsnakes/ppa -y && \
    apt-get update && \
    apt-get install -y \
    python3.14 \
    python3.14-dev \
    python3.14-venv \
    && rm -rf /var/lib/apt/lists/*

# Install pip for Python 3.14
# Python 3.14 has PEP 668 protection, so we need --break-system-packages flag
RUN curl https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py && \
    python3.14 /tmp/get-pip.py --break-system-packages && \
    rm /tmp/get-pip.py

# Set working directory
WORKDIR /app

# Copy requirements and setup files first for better caching
COPY setup.py setup.cfg tox.ini MANIFEST.in ./
COPY djanquiltdb ./djanquiltdb

# Install the package in development mode
RUN python3.14 -m pip install --upgrade pip setuptools wheel && \
    python3.14 -m pip install -e .[dev]

# Default command
CMD ["tox"]

