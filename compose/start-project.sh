#!/bin/bash
echo "Fixing permissions..."
sudo /home/glazole/msij-project/scripts/fix-permissions.sh

echo "Starting Docker Compose..."
cd /home/glazole/msij-project/compose
docker compose up -d

echo "Project started successfully!"
echo "Jupyter: http://localhost:8888"
echo "MinIO: http://localhost:9001"
echo "Spark Master: http://localhost:8080"