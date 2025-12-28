# Force x86_64 Linux even on Apple Silicon Macs
FROM --platform=linux/amd64 python:3.12-slim

# Set working directory
WORKDIR /app

# Copy project files (respects .dockerignore)
COPY . .

# Install system tools required by PyInstaller
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install PyInstaller
RUN pip install --no-cache-dir pyinstaller

# Build the Linux executable
RUN pyinstaller PaceChaserLinux.spec

# Nothing needs to run at container startup
CMD ["true"]
