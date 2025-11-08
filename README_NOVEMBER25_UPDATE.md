🔄 Обновления проекта (ноябрь 2025)

🔧 Изменения в сервисе iceberg-rest:

Добавлена явная команда запуска в docker-compose.yml для сервиса iceberg-rest с указанием classpath, включающего JDBC-драйвер PostgreSQL:

iceberg-rest:
  ...
  command: >
    java -cp "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar:/app/libs/postgresql-42.7.2.jar" \
    org.apache.iceberg.rest.RESTCatalogServer


Теперь при старте Iceberg REST Catalog сервер подхватывает драйвер PostgreSQL (postgresql-42.7.2.jar). Без этого параметра сервис iceberg-rest не мог подключиться к базе данных каталога.
📌 Почему так: Образ Iceberg REST Catalog не включает JDBC-драйвер для PostgreSQL по умолчанию. Мы примонтировали нужный JAR и добавили его в classpath запуска — благодаря этому iceberg-rest успешно соединяется с метахранилищем (PostgreSQL).

⚙️ Обновление конфигурации Spark (spark-defaults.conf):

Ресурсы кластера: увеличено количество ресурсов на задачи. Теперь у каждого Spark Executor 4 CPU-ядра и 6 ГБ памяти (плюс ~10% на overhead JVM), драйвер использует 2 ГБ памяти. Соответствующие параметры в конфигурации:

spark.executor.cores            4
spark.executor.memory           6g
spark.driver.memory             2g
spark.sql.shuffle.partitions    16


Контейнер spark-worker настроен на 4 CPU / 8 ГБ RAM, чтобы удовлетворять этим требованиям.

Adaptive Execution: включено адаптивное планирование запросов для оптимизации shuffle и join’ов на лету. Добавлены ключевые параметры:

spark.sql.adaptive.enabled                     true
spark.sql.adaptive.skewJoin.enabled            true
spark.sql.adaptive.coalescePartitions.enabled  true
spark.sql.adaptive.advisoryPartitionSizeInBytes 64m


Теперь Spark сможет автоматически объединять мелкие партиции или разбивать слишком крупные, исходя из реальных размеров данных. Это снижает риск «перекоса» данных и уменьшает издержки на shuffle при выполнении тяжёлых операций (join, groupBy и т.д.).

Event Log: для удобства отладки включено логирование событий Spark:

spark.eventLog.enabled      true
spark.eventLog.compress     true
spark.eventLog.dir          s3a://warehouse/spark-events/


История выполнения Spark-приложений (Event Log) теперь сохраняется в MinIO (в бакете warehouse/spark-events). Это позволяет просматривать Spark UI даже после завершения приложения и хранить логи вне контейнеров.

Полный путь JAR-файлов: добавлен параметр spark.jars со списком необходимых JAR’ов для Spark:

spark.jars /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,\
           /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.517.jar,\
           /opt/bitnami/spark/jars/postgresql-42.7.2.jar,\
           /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar,\
           /opt/bitnami/spark/jars/graphframes-spark3_2.12-0.9.0-spark3.5.jar


Благодаря этому все необходимые библиотеки (для S3, Iceberg, GraphFrames, JDBC-драйвер и пр.) автоматически доступны в задачах Spark. PySpark-сессии (например, в Jupyter) сразу «видят» эти JAR-файлы — не нужно вручную прописывать их при запуске.

📌 Почему так: обновлённая конфигурация Spark повышает производительность и стабильность локального кластера. Дополнительные ресурсы (RAM и CPU) позволяют обрабатывать большие объёмы данных без ошибок по памяти, а включение AQE (Adaptive Query Execution) даёт Spark возможность динамически оптимизировать план исполнения под данные, уменьшая объем shuffle и предотвращая дисбаланс партиций. Логирование событий в MinIO обеспечивает сохранение истории задач для анализа, а параметр spark.jars гарантирует, что Spark инициализируется с поддержкой Iceberg/MinIO/GraphFrames из коробки (упрощает запуск заданий и ноутбуков).

🧩 Обновление requirements.txt:

Файл зависимостей обновлён для работы с табличными данными. В него добавлены необходимые библиотеки:

pyiceberg==0.5.1  
pandas==2.3.2  
pyarrow==21.0.0  
fastparquet==2024.5.0  
openpyxl==3.1.5  
boto3==1.35.63  


Эти пакеты устанавливаются в образ Jupyter при сборке и используются в скриптах и ноутбуках:

PyIceberg 0.5.1 – Python-библиотека для работы с Iceberg Catalog (чтение/запись таблиц Iceberg из Python).

Pandas, PyArrow, FastParquet – загрузка CSV/Parquet и обработка данных (вспомогательный инструмент рядом со Spark для локальных преобразований и выгрузки результатов).

Openpyxl – позволяет экспортировать результаты в Excel (например, подготовка отчётных файлов XLSX).

Boto3 – SDK для работы с S3-совместимыми хранилищами, используется для взаимодействия с MinIO напрямую из Python.

(Кроме того, в среде Jupyter уже присутствуют pyspark==3.5.0 и graphframes-py==0.10.0, обеспечивая полный набор функций Spark и GraphFrames.)

🛠 4. Автоматизация запуска (скрипты fix-permissions.sh и start-project.sh)

После настройки конфигурации и сборки образов весь кластер можно запускать одной командой — для этого добавлены вспомогательные сценарии:

① Скрипт scripts/fix-permissions.sh – устанавливает корректные права доступа на локальные директории с конфигурацией и данными перед запуском контейнеров:

#!/bin/bash
sudo chown -R 1001:1001 /home/<user>/msij-project/conf/spark
sudo chmod -R 755 /home/<user>/msij-project/conf/spark
sudo chown -R 1001:1001 /home/<user>/msij-project/work
sudo chmod -R u+rwX,g+rwX /home/<user>/msij-project/work
echo "Permissions fixed successfully"


Замените <user> на своё имя пользователя. UID/GID 1001:1001 соответствует пользователю Spark в контейнерах (в образах Bitnami Spark по умолчанию именно такой UID).

📌 Почему это нужно: при монтировании папок хоста внутрь Docker-контейнеров они зачастую имеют владельца root. В нашем случае процессы Spark/Jupyter внутри контейнеров запускаются от пользователя с UID 1001, поэтому без изменения прав они не смогут создавать файлы (например, писать логи Spark или результаты работы) в директории conf/ и work/. Скрипт fix-permissions.sh рекурсивно меняет владельца и права: теперь пользователь Spark (1001) может читать и записывать конфигурации и данные. Выполнение этого скрипта перед стартом кластера гарантирует, что проблемы с доступом к файлам не возникнут.

📘 Добавление в sudoers: чтобы fix-permissions.sh запускался без запроса пароля, можно разрешить текущему пользователю выполнять этот скрипт от root. Откройте конфигурацию sudo через sudo visudo и добавьте строку (заменив <user> на своё имя):

<user> ALL=(root) NOPASSWD: /home/<user>/msij-project/scripts/fix-permissions.sh


Так вы позволите запускать скрипт с повышенными правами без ввода пароля. ⚠️ Важно: в записи выше указана конкретная команда – другой скрипт или команда от имени root по-прежнему потребуют пароль. Убедитесь, что путь указан верно и покрывает только нужный сценарий, во избежание рисков безопасности.

② Скрипт compose/start-project.sh – выполняет полный запуск проекта:

#!/bin/bash
echo "Fixing permissions..."
sudo /home/<user>/msij-project/scripts/fix-permissions.sh

echo "Starting Docker Compose..."
cd /home/<user>/msij-project/compose
docker compose up -d

echo "Project started successfully!"
echo "Jupyter:    http://localhost:8888"
echo "MinIO:      http://localhost:9001"
echo "Spark UI:   http://localhost:8080"


Запустите этот сценарий из WSL/терминала (например, командой bash compose/start-project.sh из корня проекта). Он автоматически:

Исправляет права на каталогах conf/ и work/ (вызывает fix-permissions.sh).

Стартует Docker Compose — поднимает все сервисы кластера командой docker compose up -d.

Выводит адреса основных сервисов:

Jupyter Lab – интерфейс ноутбуков на http://localhost:8888 (токен доступа задан в docker-compose.yml как "lab", вход без пароля).

MinIO Console – веб-интерфейс хранилища на http://localhost:9001 (учётные данные: minio/minio_minio).

Spark Master UI – статус-контроль Spark-кластера на http://localhost:8080 (информация о Worker’ах, задачах и памяти).

✅ Теперь для запуска всей инфраструктуры достаточно одной команды. Сценарий start-project.sh последовательно выполняет необходимые подготовительные шаги и запускает кластер, поэтому вам не нужно каждый раз вручную менять права или вводить длинные команды Docker — после быстрого старта можно сразу переходить к работе в Jupyter или запуску Spark-скриптов, зная, что все сервисы подняты и правильно сконфигурированы.

Вот отформатированный фрагмент в Markdown, готовый к вставке в README.md:

````markdown
## 🔄 Обновления проекта (ноябрь 2025)

### 🔧 Изменения в сервисе `iceberg-rest`

Добавлена явная команда запуска в `docker-compose.yml` для сервиса `iceberg-rest` с указанием classpath, включающего JDBC-драйвер PostgreSQL:

```yaml
iceberg-rest:
  ...
  command: >
    java -cp "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar:/app/libs/postgresql-42.7.2.jar" \
    org.apache.iceberg.rest.RESTCatalogServer
````

Теперь при старте **Iceberg REST Catalog** сервер подхватывает драйвер **PostgreSQL** (`postgresql-42.7.2.jar`). Без этого параметра сервис `iceberg-rest` не мог подключиться к базе данных каталога.

📌 **Почему так:** образ Iceberg REST Catalog не включает JDBC-драйвер для PostgreSQL по умолчанию. Мы примонтировали нужный JAR и добавили его в classpath запуска — благодаря этому `iceberg-rest` успешно соединяется с метахранилищем (PostgreSQL).

---

### ⚙️ Обновление конфигурации Spark (`spark-defaults.conf`)

#### 💾 Ресурсы кластера

```conf
spark.executor.cores            4
spark.executor.memory           6g
spark.driver.memory             2g
spark.sql.shuffle.partitions    16
```

Контейнер `spark-worker` настроен на 4 CPU / 8 ГБ RAM, чтобы удовлетворять этим требованиям.

#### 🧠 Adaptive Execution

```conf
spark.sql.adaptive.enabled                     true
spark.sql.adaptive.skewJoin.enabled            true
spark.sql.adaptive.coalescePartitions.enabled  true
spark.sql.adaptive.advisoryPartitionSizeInBytes 64m
```

Теперь Spark может автоматически объединять мелкие партиции и разбивать слишком крупные — снижая нагрузку на shuffle и увеличивая стабильность обработки.

#### 📑 Event Log

```conf
spark.eventLog.enabled      true
spark.eventLog.compress     true
spark.eventLog.dir          s3a://warehouse/spark-events/
```

История выполнения Spark-приложений теперь сохраняется в MinIO и доступна для просмотра даже после завершения задач.

#### 📦 Полный путь JAR-файлов

```conf
spark.jars /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,\
           /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.517.jar,\
           /opt/bitnami/spark/jars/postgresql-42.7.2.jar,\
           /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar,\
           /opt/bitnami/spark/jars/graphframes-spark3_2.12-0.9.0-spark3.5.jar
```

📌 **Почему так:** обновлённая конфигурация Spark повышает производительность и стабильность. Все зависимости доступны сразу, без ручного указания при запуске `spark-submit`.

---

### 🧩 Обновление `requirements.txt`

```text
pyiceberg==0.5.1  
pandas==2.3.2  
pyarrow==21.0.0  
fastparquet==2024.5.0  
openpyxl==3.1.5  
boto3==1.35.63  
```

📌 Эти библиотеки:

* `pyiceberg` — доступ к Iceberg Catalog из Python;
* `pandas`, `pyarrow`, `fastparquet` — загрузка/сериализация табличных данных;
* `openpyxl` — экспорт в Excel;
* `boto3` — работа с MinIO/S3 через Python SDK.

> Также доступны `pyspark==3.5.0` и `graphframes-py==0.10.0`.

---

## 🛠 Автоматизация запуска (скрипты `fix-permissions.sh` и `start-project.sh`)

### ✅ Скрипт `scripts/fix-permissions.sh`

```bash
#!/bin/bash
sudo chown -R 1001:1001 /home/<user>/msij-project/conf/spark
sudo chmod -R 755 /home/<user>/msij-project/conf/spark
sudo chown -R 1001:1001 /home/<user>/msij-project/work
sudo chmod -R u+rwX,g+rwX /home/<user>/msij-project/work
echo "Permissions fixed successfully"
```


📘 Сделать исполняемым:

```bash
sudo chmod +x /home/glazole/msij-project/scripts/fix-permissions.sh
```

📌 **Почему это важно:**
Контейнеры запускаются от пользователя `uid=1001`, а директории монтируются от `root`. Без изменения прав возможны ошибки записи:

```
Permission denied: Failed to create staging directory under /work...
```

---

### 🔐 Добавление в `sudoers`

Откройте `visudo`:

```bash
sudo visudo
```

И добавьте строку:

```text
<user> ALL=(root) NOPASSWD: /home/<user>/msij-project/scripts/fix-permissions.sh
```

> ⚠️ Это даст разрешение только на этот скрипт. Всё остальное потребует пароль.

---

### 🚀 Скрипт `compose/start-project.sh`

```bash
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
```

📘 Сделать исполняемым:

```bash
sudo chmod +x start-project.sh
```


📘 Запуск:

```bash
# Запуск проекта
./start-project.sh --start

# Остановка проекта  
./start-project.sh --stop

# Перезапуск проекта
./start-project.sh --restart

# Показать статус сервисов
./start-project.sh --status

# Показать логи всех сервисов
./start-project.sh --logs

# Показать логи конкретного сервиса
./start-project.sh --logs jupyter
./start-project.sh --logs spark-master
./start-project.sh --logs iceberg-rest

# Показать помощь
./start-project.sh --help
```


🧩 Скрипт:

1. Исправляет права на `conf/` и `work/`;
2. Запускает `docker-compose up -d`;
3. Выводит адреса интерфейсов:

   * **JupyterLab**: [http://localhost:8888](http://localhost:8888)
   * **MinIO**: [http://localhost:9001](http://localhost:9001) (логин: `minio`, пароль: `minio_minio`)
   * **Spark Master UI**: [http://localhost:8080](http://localhost:8080)

✅ **Теперь запуск проекта — в один клик.**

```
```
