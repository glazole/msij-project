import os, zipfile, tempfile, traceback, sys, shutil
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

ZIP_DIR      = Path(os.environ.get("ZIP_DIR", "/win_df"))
CSV_SEP      = os.environ.get("CSV_SEP", ",")
CSV_HEADER   = os.environ.get("CSV_HEADER", "true")
INFER_SCHEMA = os.environ.get("INFER_SCHEMA", "true")
CSV_ENCODING = os.environ.get("CSV_ENCODING", "UTF-8")

def sanitize_table_name(name: str) -> str:
    """Преобразует имя архива в допустимое имя таблицы"""
    return name.lower().replace("-", "_").replace(".", "_")

def first_csv_from_zip(zp: Path, dst_dir: Path) -> Path | None:
    with zipfile.ZipFile(zp, "r") as z:
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not names:
            return None
        src = names[0]
        dst_path = dst_dir / f"{zp.stem}.csv"
        with z.open(src, "r") as fin, open(dst_path, "wb") as fout:
            shutil.copyfileobj(fin, fout, length=16 * 1024 * 1024)
        return dst_path

if __name__ == "__main__":
    spark = SparkSession.builder.appName("zip->iceberg(split)").getOrCreate()
    zip_paths = sorted(p for p in ZIP_DIR.glob("*.zip"))
    total = len(zip_paths)
    print(f"👉 [INFO] Found {total} zip files in {ZIP_DIR}", flush=True)
    if total == 0:
        spark.stop(); sys.exit(0)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.bronze")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS ice.bronze.crpt_load_log (
          zip_name string,
          table_name string,
          loaded_at timestamp
        ) USING iceberg
    """)

    processed = skipped = failed = 0
    for i, zp in enumerate(zip_paths, 1):
        try:
            name = zp.name
            table_suffix = sanitize_table_name(zp.stem)
            table_name = f"ice.bronze.{table_suffix}"
            esc = name.replace("'", "''")

            already = spark.sql(
                f"SELECT 1 FROM ice.bronze.crpt_load_log WHERE zip_name='{esc}' LIMIT 1"
            ).count() > 0
            if already:
                print(f"[SKIP {i}/{total}] {name}: already logged", flush=True)
                skipped += 1
                continue

            print(f"[START {i}/{total}] {name}", flush=True)
            tmpdir = Path(tempfile.mkdtemp(prefix="csv_as_is_"))
            csv_path = first_csv_from_zip(zp, tmpdir)
            if not csv_path or not csv_path.exists():
                print(f"[WARN  {i}/{total}] {name}: no CSV inside", flush=True)
                skipped += 1
                continue

            size_mb = round(csv_path.stat().st_size / (1024*1024), 2)
            print(f"[EXTRACT {i}/{total}] -> {csv_path.name} ({size_mb} MB)", flush=True)
            print(f"✅ [CHECK] File exists: {csv_path.exists()} — {csv_path}")
            print(f"🧭 [URI] {csv_path.as_uri()}")

            df = (spark.read
                    .option("sep", CSV_SEP)
                    .option("header", CSV_HEADER)
                    .option("inferSchema", INFER_SCHEMA)
                    .option("encoding", CSV_ENCODING)
                    .csv(csv_path.as_uri())

            table_exists = spark._jsparkSession.catalog().tableExists(table_name)

            if not table_exists:
                df.writeTo(table_name).create()
                print(f"👉 [CREATE] {table_name}", flush=True)

            df.writeTo(table_name).append()
            print(f"👉 [APPEND] {name} -> {table_name}", flush=True)

            (spark.createDataFrame([(name, table_name)], ["zip_name", "table_name"])
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
                for p in tmpdir.glob("*"):
                    try: p.unlink()
                    except: pass
                tmpdir.rmdir()
            except Exception:
                pass

    logged = spark.sql("SELECT COUNT(*) AS c FROM ice.bronze.crpt_load_log").first()["c"]
    print("===== SUMMARY =====", flush=True)
    print(f"found      : {total}", flush=True)
    print(f"processed  : {processed}", flush=True)
    print(f"skipped    : {skipped}", flush=True)
    print(f"failed     : {failed}", flush=True)
    print(f"logged rows: {logged}", flush=True)
    print("===================", flush=True)
    spark.stop()
    print("✅ [DONE]", flush=True)