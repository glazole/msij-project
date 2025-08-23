import os, zipfile, tempfile, traceback, sys, shutil
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

ZIP_DIR      = Path(os.environ.get("ZIP_DIR", "/win_df"))
CSV_SEP      = os.environ.get("CSV_SEP", ",")
CSV_HEADER   = os.environ.get("CSV_HEADER", "true")
INFER_SCHEMA = os.environ.get("INFER_SCHEMA", "true")
CSV_ENCODING = os.environ.get("CSV_ENCODING", "UTF-8")
TABLE_NAME   = os.environ.get("TABLE_NAME", "ice.bronze.crpt_2025_raw")

# --- STREAM COPY: копируем по кускам, без чтения (.read()) всего файла ---
def first_csv_from_zip(zp: Path, dst_dir: Path) -> Path | None:
    """
    Извлекает первый найденный CSV-файл из архива ZIP и сохраняет его в указанную директорию.

    Параметры:
        zp (Path): Путь к ZIP-архиву, из которого будет извлечен CSV-файл.
        dst_dir (Path): Директория, в которую будет сохранен извлеченный CSV-файл.

    Возвращает:
        Path | None: Путь к созданному CSV-файлу в случае успеха, 
                     или None, если CSV-файлы в архиве отсутствуют.

    Исключения:
        - Может вызвать исключение при повреждении ZIP-архива.
        - Может вызвать исключение при отсутствии прав на чтение/запись файлов.

    Примечание:
        Используется буфер размером 16 МБ для эффективного копирования данных.
        Имя выходного файла формируется как имя исходного ZIP-файла с расширением .csv.
    """
    with zipfile.ZipFile(zp, "r") as z:
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not names:
            return None
        src = names[0]
        dst_path = dst_dir / f"{zp.stem}.csv"
        with z.open(src, "r") as fin, open(dst_path, "wb") as fout:
            shutil.copyfileobj(fin, fout, length=16 * 1024 * 1024)  # 16MB буфер
        return dst_path

if __name__ == "__main__":
    # можно чуть помочь Iceberg на случай расхождений схем
    # Spark 3.5 + Iceberg 1.6 понимают этот флаг
    # (оставь закомментированным, если не нужно)
    # spark.conf.set("spark.sql.iceberg.merge-schema", "true")

    spark = (SparkSession.builder.appName("zip->iceberg(as-is)").getOrCreate())

    zip_paths = sorted(p for p in ZIP_DIR.glob("*.zip"))
    total = len(zip_paths)
    print(f"👉 [INFO] Found {total} zip files in {ZIP_DIR}", flush=True)
    if total == 0:
        print("🙅‍♂️ Archives are not found — do not create anything, exit")
        spark.stop(); sys.exit(0)
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.bronze")

    # создаем таблицу iceberg для логов, если ее нет
    spark.sql("""
        CREATE TABLE IF NOT EXISTS ice.bronze.crpt_load_log (
          zip_name string,
          loaded_at timestamp
        ) USING iceberg
    """)

    table_exists = spark._jsparkSession.catalog().tableExists(TABLE_NAME)

    # сначала проверяем, не загружали ли мы этот архив ранее
    processed = skipped = failed = 0
    for i, zp in enumerate(zip_paths, 1):
        try:
            name = zp.name
            esc = name.replace("'", "''")
            already = spark.sql(
                f"SELECT 1 FROM ice.bronze.crpt_load_log WHERE zip_name='{esc}' LIMIT 1"
            ).count() > 0
            if already:
                # если уже загружали, то пропускаем
                print(f"[SKIP {i}/{total}] {name}: already logged", flush=True)
                skipped += 1
                continue

            # если не загружали, то разархивируем во временную директорию, 
            # создаем iceberg-таблицу и загружаем данные
            print(f"[START {i}/{total}] {name}", flush=True)
            tmpdir = Path(tempfile.mkdtemp(prefix="csv_as_is_"))
            # проверяем, есть ли в архиве хотя бы один CSV
            csv_path = first_csv_from_zip(zp, tmpdir)
            if not csv_path or not csv_path.exists():
                print(f"[WARN  {i}/{total}] {name}: no CSV inside", flush=True)
                skipped += 1
                continue
            # проверяем размер файла
            size_mb = round(csv_path.stat().st_size / (1024*1024), 2)
            print(f"[EXTRACT {i}/{total}] -> {csv_path.name} ({size_mb} MB)", flush=True)
            # читаем данные из CSV
            df = (spark.read
                    .option("sep", CSV_SEP)
                    .option("header", CSV_HEADER)
                    .option("inferSchema", INFER_SCHEMA)
                    .option("encoding", CSV_ENCODING)
                    .csv(str(csv_path)))
            # если еще не создана, то создаем iceberg-таблицу
            if not table_exists:
                df.writeTo(TABLE_NAME).create()
                table_exists = True
                print(f"👉 [CREATE] {TABLE_NAME}", flush=True)
            # загружаем данные
            df.writeTo(TABLE_NAME).append()
            print(f"👉 [APPEND] {name} -> {TABLE_NAME}", flush=True)
            # записываем имя файла в лог
            (spark.createDataFrame([(name,)], ["zip_name"])
                 .withColumn("loaded_at", current_timestamp())
                 .writeTo("ice.bronze.crpt_load_log").append())

            processed += 1
            print(f"[DONE  {i}/{total}] {name}", flush=True)

        except Exception as e:
            failed += 1
            print(f"[FAIL  {i}/{total}] {zp.name}: {e}", flush=True)
            traceback.print_exc()
        finally:
            try:
                # удаляем временные файлы
                for p in tmpdir.glob("*"):
                    try: p.unlink()
                    except: pass
                tmpdir.rmdir()
            except Exception:
                pass
    # логируем общее количество обработанных файлов и записанных строк в консоль
    logged = spark.sql("SELECT COUNT(*) AS c FROM ice.bronze.crpt_load_log").first()["c"]
    print("===== SUMMARY =====", flush=True)
    print(f"found      : {total}", flush=True)
    print(f"processed  : {processed}", flush=True)
    print(f"skipped    : {skipped}", flush=True)
    print(f"failed     : {failed}", flush=True)
    print(f"logged rows: {logged}", flush=True)
    print("===================", flush=True)
    # завершаем работу
    spark.stop()
    print("✅ [DONE]", flush=True)