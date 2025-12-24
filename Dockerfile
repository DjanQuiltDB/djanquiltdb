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

# Install Python 3.12 from Ubuntu repos and Python 3.11 from deadsnakes PPA
# Python 3.12 is already available in Ubuntu 24.04, so we only need deadsnakes for 3.11
RUN add-apt-repository ppa:deadsnakes/ppa -y && \
    apt-get update && \
    apt-get install -y \
    python3.11 \
    python3.11-dev \
    python3.11-distutils \
    python3.12 \
    python3.12-dev \
    python3.12-venv \
    && rm -rf /var/lib/apt/lists/*

# Install pip for each Python version
# Python 3.12 has PEP 668 protection, so we need --break-system-packages flag
RUN curl https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py && \
    python3.11 /tmp/get-pip.py && \
    python3.12 /tmp/get-pip.py --break-system-packages && \
    rm /tmp/get-pip.py

# Tox can find Python versions directly via python3.11 and python3.12 commands
# No need for update-alternatives as the binaries are already in PATH

# Set working directory
WORKDIR /app

# Copy requirements and setup files first for better caching
COPY setup.py setup.cfg tox.ini MANIFEST.in .bandit ./
COPY djanquiltdb ./djanquiltdb

# Install the package in development mode
RUN python3.11 -m pip install --upgrade pip setuptools wheel && \
    python3.11 -m pip install -e .[dev]

# Default command
CMD ["tox"]

