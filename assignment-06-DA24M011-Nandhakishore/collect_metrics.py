#!/usr/bin/env python3

import time
import subprocess
import logging
import os
from prometheus_client import start_http_server, Gauge
import re

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Configure logging to save in the same directory as the script
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG to capture more details
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=os.path.join(script_dir, 'system_metrics.log')
)
logger = logging.getLogger(__name__)

# Define Prometheus metrics for I/O statistics
io_read_rate = Gauge('io_read_rate', 'Disk read rate in operations per second', ['device'])
io_write_rate = Gauge('io_write_rate', 'Disk write rate in operations per second', ['device'])
io_tps = Gauge('io_tps', 'Transactions per second', ['device'])
io_read_bytes = Gauge('io_read_bytes', 'Disk read bytes per second', ['device'])
io_write_bytes = Gauge('io_write_bytes', 'Disk write bytes per second', ['device'])
cpu_avg_percent = Gauge('cpu_avg_percent', 'CPU utilization percentage', ['mode'])

# Dictionary to store dynamically created memory metrics
meminfo_metrics = {}

# Collect disk I/O and CPU statistics using iostat command and update Prometheus metrics
def collect_io_stats():    
    try:
        # Execute iostat command with detailed output (-x) and CPU stats (-c)
        result = subprocess.run(['iostat', '-d', '-x', '-c'], 
                              capture_output=True, 
                              text=True, 
                              check=True)
        
        logger.info("Successfully collected iostat data")
        logger.debug(f"Full iostat output:\n{result.stdout}")
        
        # Parse iostat output
        lines = result.stdout.splitlines()
        parsing_disk = False
        parsing_cpu = False
        
        # Track devices to ensure all metrics are set, even if no data
        devices_seen = set()
        
        for line in lines:
            if not line.strip():
                continue
                
            # Switch between disk and CPU sections
            if 'Device' in line:
                parsing_disk = True
                parsing_cpu = False
                continue
            elif 'avg-cpu' in line:
                parsing_disk = False
                parsing_cpu = True
                continue
                
            fields = line.split()
            
            # Parse disk statistics
            if parsing_disk and len(fields) >= 14:
                device = fields[0]
                devices_seen.add(device)
                read_rate = float(fields[2])    # r/s (reads per second)
                write_rate = float(fields[3])   # w/s (writes per second)
                read_kb = float(fields[4])      # rkB/s
                write_kb = float(fields[5])     # wkB/s
                tps = float(fields[13])         # tps
                
                logger.debug(f"Device {device}: r/s={read_rate}, w/s={write_rate}, rkB/s={read_kb}, wkB/s={write_kb}, tps={tps}")
                
                io_read_rate.labels(device=device).set(read_rate)
                io_write_rate.labels(device=device).set(write_rate)
                io_tps.labels(device=device).set(tps)
                io_read_bytes.labels(device=device).set(read_kb * 1024)
                io_write_bytes.labels(device=device).set(write_kb * 1024)
                
            # Parse CPU statistics
            if parsing_cpu and len(fields) >= 6:
                cpu_avg_percent.labels(mode='user').set(float(fields[0]))
                cpu_avg_percent.labels(mode='nice').set(float(fields[1]))
                cpu_avg_percent.labels(mode='system').set(float(fields[2]))
                cpu_avg_percent.labels(mode='iowait').set(float(fields[4]))
                cpu_avg_percent.labels(mode='idle').set(float(fields[5]))
                logger.debug(f"CPU stats: user={fields[0]}, nice={fields[1]}, system={fields[2]}, iowait={fields[4]}, idle={fields[5]}")
                
        # Ensure all metrics are set for all seen devices (default to 0 if not updated)
        for device in devices_seen:
            if not io_read_rate.labels(device=device)._value.get():
                io_read_rate.labels(device=device).set(0)
                logger.debug(f"Set io_read_rate for {device} to 0 as no value was parsed")
            if not io_write_rate.labels(device=device)._value.get():
                io_write_rate.labels(device=device).set(0)
                logger.debug(f"Set io_write_rate for {device} to 0 as no value was parsed")
                
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to collect iostat data: {e}")
    except Exception as e:
        logger.error(f"Error processing iostat data: {e}")

# Collect memory statistics from /proc/meminfo and update Prometheus metrics
def collect_meminfo_stats():
    try:
        with open('/proc/meminfo', 'r') as f:
            meminfo = f.read()
            
        logger.info("Successfully collected meminfo data")
        
        # Parse each line of meminfo
        for line in meminfo.splitlines():
            if not line.strip():
                continue
                
            # Extract metric name and value
            match = re.match(r'(\w+):\s+(\d+)(?:\s+kB)?', line)
            if match:
                metric_name, value = match.groups()
                # Convert to snake_case and add prefix
                metric_key = f"meminfo_{metric_name.lower()}"
                
                # Create gauge if it doesn't exist
                if metric_key not in meminfo_metrics:
                    meminfo_metrics[metric_key] = Gauge(
                        metric_key,
                        f'Memory statistic: {metric_name}',
                        []
                    )
                
                # Set the value (convert kB to bytes)
                meminfo_metrics[metric_key].set(float(value) * 1024)
                logger.debug(f"Set {metric_key} to {float(value) * 1024}")
                
    except FileNotFoundError:
        logger.error("Could not open /proc/meminfo")
    except Exception as e:
        logger.error(f"Error processing meminfo data: {e}")

def main():
    # Start Prometheus metrics server on port 18000
    start_http_server(18000)
    logger.info("Started Prometheus metrics server on port 18000")
    
    # Continuous metrics collection
    while True:
        collect_io_stats()
        collect_meminfo_stats()
        time.sleep(2)  # Collect 2 seconds

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Metrics collection stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")