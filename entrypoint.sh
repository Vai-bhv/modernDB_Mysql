#!/bin/bash

# Wait for MySQL to be ready
echo "Waiting for MySQL to start..."
while ! mysqladmin ping -h mysql -u root --password=$MYSQL_ROOT_PASSWORD --silent; do
    sleep 1
done
echo "MySQL is up and running!"

# Execute the Python scripts
echo "Running data loading script..."
python3 /usr/src/app/load_data.py

echo "Running benchmark script..."
python3 /usr/src/app/benchmark.py

# Keep container running
wait $!
