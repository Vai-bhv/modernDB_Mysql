# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory
WORKDIR /usr/src/app

# Install MySQL client
RUN apt-get update && apt-get install -y default-mysql-client && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install pymysql cryptography

# Copy the Python scripts and data files into the container
COPY load_data.py .
COPY benchmark.py .
COPY filtered_titled_basics.json .
COPY filtered_title.basics.tsv .

# Copy and set permissions for the entrypoint script
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Set the entrypoint script to run when the container starts
ENTRYPOINT ["./entrypoint.sh"]
