# Use a lightweight Ubuntu-based image
FROM ubuntu:20.04

# Set environment variables to avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary Linux packages and Python 3.9
RUN apt-get update && apt-get install -y \
    python3.9 \
    python3-pip \
    sysstat  # Provides the 'iostat' command \
    && rm -rf /var/lib/apt/lists/*  # Clean up to reduce image size

# Set the working directory
WORKDIR /app

# Copy the script and requirements file into the container
COPY collect_metrics.py /app/prometheus_exporter.py 
COPY requirements.txt /app/requirements.txt

# Install required Python packages
RUN python3.9 -m pip install -r requirements.txt

# Expose the Prometheus metrics port
EXPOSE 18000

# Run the script with Python 3.9
CMD ["python3.9", "/app/prometheus_exporter.py"]