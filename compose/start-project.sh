# #!/bin/bash
# echo "Fixing permissions..."
# sudo /home/glazole/msij-project/scripts/fix-permissions.sh

# echo "Starting Docker Compose..."
# cd /home/glazole/msij-project/compose
# docker compose up -d

# echo "Project started successfully!"
# echo "Jupyter: http://localhost:8888"
# echo "MinIO: http://localhost:9001"
# echo "Spark Master: http://localhost:8080"

#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SCRIPTS_DIR="$PROJECT_ROOT/scripts"

# Функция для исправления прав
fix_permissions() {
    echo "Fixing permissions..."
    sudo /home/glazole/msij-project/scripts/fix-permissions.sh
    echo "Permissions fixed successfully"
}

# Функция запуска проекта
start_project() {
    fix_permissions
    
    echo "Starting Docker Compose services..."
    docker compose up -d
    
    echo "Waiting for services to start..."
    sleep 10
    
    # Проверяем статус сервисов
    echo "Service status:"
    docker compose ps -a
    
    echo ""
    echo "=== Project started successfully! ==="
    echo "Jupyter Lab: http://localhost:8888 (token: lab)"
    echo "MinIO Console: http://localhost:9001 (user: minio, password: minio_minio)"
    echo "Spark Master: http://localhost:8080"
    echo "Spark Worker: http://localhost:8081"
    echo "Iceberg REST: http://localhost:8181"
    echo "PostgreSQL: localhost:5432 (user: iceberg, password: iceberg)"
}

# Функция остановки проекта
stop_project() {
    echo "Stopping Docker Compose services..."
    docker compose down
    
    echo "=== Project stopped successfully! ==="
}

# Функция перезапуска проекта
restart_project() {
    echo "Restarting project..."
    stop_project
    sleep 5
    start_project
}

# Функция показа статуса
status_project() {
    echo "Current project status:"
    docker compose ps -a
    echo ""
    echo "Service URLs:"
    echo "Jupyter Lab: http://localhost:8888"
    echo "MinIO Console: http://localhost:9001"
    echo "Spark Master: http://localhost:8080"
}

# Функция показа логов
logs_project() {
    local service=$1
    if [ -n "$service" ]; then
        echo "Showing logs for service: $service"
        docker compose logs -f "$service"
    else
        echo "Showing logs for all services:"
        docker compose logs -f
    fi
}

# Функция помощи
show_help() {
    echo "Usage: $0 {--start|--stop|--restart|--status|--logs [service]|--help}"
    echo ""
    echo "Options:"
    echo "  --start       Start all services"
    echo "  --stop        Stop all services"
    echo "  --restart     Restart all services"
    echo "  --status      Show service status"
    echo "  --logs [svc]  Show logs (all or specific service)"
    echo "  --help        Show this help message"
    echo ""
    echo "Available services: minio, mc, spark-master, spark-worker, jupyter, postgresql, iceberg-rest"
}

# Основная логика
case "$1" in
    --start)
        start_project
        ;;
    --stop)
        stop_project
        ;;
    --restart)
        restart_project
        ;;
    --status)
        status_project
        ;;
    --logs)
        logs_project "$2"
        ;;
    --help|-h)
        show_help
        ;;
    *)
        echo "Error: Unknown option '$1'"
        echo ""
        show_help
        exit 1
        ;;
esac