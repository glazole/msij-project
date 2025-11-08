#!/bin/bash
sudo chown -R 1001:1001 /home/glazole/msij-project/conf/spark
sudo chmod -R 755 /home/glazole/msij-project/conf/spark
sudo chown -R 1001:1001 /home/glazole/msij-project/work
sudo chmod -R u+rwX,g+rwX /home/glazole/msij-project/work
echo "Permissions fixed successfully"