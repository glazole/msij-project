import os, sys, traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import boto3

# === Константы ===
EXT_ENDPOINT  = "https://minio-api.data-office-mz.minzdrav.gov.ru"   # источник parquet
EXT_BUCKET    = "db1lecsredstva"
EXT_PREFIX    = "raw/"

ICE_ENDPOINT  = "http://minio:9000"                                  # хранилище Iceberg
ICE_WAREHOUSE = "s3a://warehouse/iceberg/"
TABLE_NAME    = "ice.bronze.f2_out"
LOG_TABLE     = "ice.bronze.load_log"

# === SparkSession ===
spark = (
    SparkSession.builder
        .appName("extS3_to_iceberg")
        # Каталог Iceberg через REST
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "rest")
        .config("spark.sql.catalog.ice.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.ice.warehouse", ICE_WAREHOUSE)
        # 🔹 Настройки для Iceberg S3 (внутренний MinIO)
        .config("spark.hadoop.fs.s3a.endpoint", ICE_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio_minio")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # 🔹 Дополнительная схема для внешнего MinIO (чтобы читать parquet)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.ext.endpoint", EXT_ENDPOINT)
        .config("spark.hadoop.fs.s3a.ext.access.key", "8W3zVpjhoQoyugqT1p0o")
        .config("spark.hadoop.fs.s3a.ext.secret.key", "yGSczjB8shtKwuc5IdRLuiTxNkyRkVjsjxkdXEHt")
        .config("spark.hadoop.fs.s3a.ext.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.ext.connection.ssl.enabled", "true")
        .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.bronze")

# === Лог-таблица ===
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
  file_path string,
  loaded_at timestamp
) USING iceberg
""")

# === Получаем список parquet из внешнего MinIO ===
s3 = boto3.client(
    "s3",
    endpoint_url=EXT_ENDPOINT,
    aws_access_key_id="8W3zVpjhoQoyugqT1p0o",
    aws_secret_access_key="yGSczjB8shtKwuc5IdRLuiTxNkyRkVjsjxkdXEHt",
)

response = s3.list_objects_v2(Bucket=EXT_BUCKET, Prefix=EXT_PREFIX)
if "Contents" not in response:
    print(f"🙅‍♂️ Нет файлов по префиксу {EXT_PREFIX}")
    spark.stop()
    sys.exit(0)

files = [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".parquet")]
print(f"👉 Найдено {len(files)} parquet-файлов в s3://{EXT_BUCKET}/{EXT_PREFIX}")

processed = skipped = failed = 0
table_exists = spark._jsparkSession.catalog().tableExists(TABLE_NAME)

# === Основной цикл ===
for i, key in enumerate(files, 1):
    try:
        file_path = f"s3a://{EXT_BUCKET}/{key}"
        # Проверка, не загружен ли уже
        esc = file_path.replace("'", "''")
        already = spark.sql(f"SELECT 1 FROM {LOG_TABLE} WHERE file_path='{esc}' LIMIT 1").count() > 0
        if already:
            print(f"[SKIP {i}] {key} уже загружен ранее")
            skipped += 1
            continue

        print(f"[LOAD {i}] {file_path}")
        # Чтение parquet из внешнего MinIO
        df = (spark.read
                .option("fs.s3a.endpoint", EXT_ENDPOINT)
                .option("fs.s3a.access.key", "8W3zVpjhoQoyugqT1p0o")
                .option("fs.s3a.secret.key", "yGSczjB8shtKwuc5IdRLuiTxNkyRkVjsjxkdXEHt")
                .option("fs.s3a.connection.ssl.enabled", "true")
                .parquet(file_path))

        if not table_exists:
            df.writeTo(TABLE_NAME).create()
            table_exists = True
            print(f"👉 [CREATE] {TABLE_NAME}")

        df.writeTo(TABLE_NAME).append()
        print(f"👉 [APPEND] {key}")

        # лог
        (
            spark.createDataFrame([(file_path,)], ["file_path"])
            .withColumn("loaded_at", current_timestamp())
            .writeTo(LOG_TABLE)
            .append()
        )
        processed += 1

    except Exception as e:
        failed += 1
        print(f"[FAIL {i}] {key}: {e}")
        traceback.print_exc()

# === Итог ===
logged = spark.sql(f"SELECT COUNT(*) AS c FROM {LOG_TABLE}").first()["c"]
print("===== SUMMARY =====")
print(f"processed : {processed}")
print(f"skipped   : {skipped}")
print(f"failed    : {failed}")
print(f"logged    : {logged}")
print("===================")
spark.stop()
print("✅ [DONE]")