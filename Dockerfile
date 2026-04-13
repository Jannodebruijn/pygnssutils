# Use an official Python image as the base image
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Copy the contents of the pygnssutils folder to the image
COPY pyproject.toml README.md LICENSE  ./
COPY src/ ./src/

# Install Poetry, project dependencies, and net-tools for health checks
# Remove apt lists to reduce image size
RUN apt-get update && \
    apt-get install -y --no-install-recommends net-tools && \
    rm -rf /var/lib/apt/lists/* && \
    pip install poetry==2.2 && \ 
    poetry config virtualenvs.create false && \
    poetry install --no-interaction --without build,test,deploy

# Create directories for data and logs
RUN mkdir -p /data /logs

# Copy configuration file
COPY gnssstreamer.conf ./

# Set environment variables
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

# Default command (can be overridden)
CMD ["poetry", "run", "gnssstreamer", "--config", "gnssstreamer.conf"]
