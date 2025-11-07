import os, zipfile, tempfile, traceback, sys, shutil, unicodedata, re
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

ZIP_DIR      = Path(os.environ.get("ZIP_DIR", "/win_df"))
CSV_SEP      = os.environ.get("CSV_SEP", ",")
CSV_HEADER   = os.environ.get("CSV_HEADER", "true")
INFER_SCHEMA = os.environ.get("INFER_SCHEMA", "true")
CSV_ENCODING = os.environ.get("CSV_ENCODING", "UTF-8")

def strip_accents(text):
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def safe_ascii_name(name: str) -> str:
    base = Path(name).stem
    base = strip_accents(base)
    base = base.encode("ascii", errors="ignore").decode()
    base = base.lower()
    base = base.replace(" ", "_").replace("-", "_").replace(".", "_")
    return re.sub(r"[^a-z0-9_]", "", base)

def first_csv_from_zip(zp: Path, dst_dir: Path) -> Path | None:
    with zipfile.ZipFile(zp, "r") as z:
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not names:
            print(f"⚠️  {zp.name} — no CSV found in archive", flush=True)
            return None
        src = names[0]
        filename_inside = Path(src).name
        dst_path = dst_dir / filename_inside
        with z.open(src, "r") as fin, open(dst_path, "wb") as fout:
            shutil.copyfileobj(fin, fout, length=16 * 1024 * 1024)
        size = dst_path.stat().st_size
        print(f"📄 Extracted: {dst_path.name} — size: {round(size/1024, 2)} KB", flush=True)
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
        CREATE TABLE IF NOT EXISTS ice.bronze.load_log (
          zip_name string,
          loaded_at timestamp
        ) USING iceberg
    """)

    MAPPING = {
        "Таблица_4": "f4_sprav_gtin",
        "Таблица_5": "f5_sprav_md",
        "Таблица_6": "f6_sprav_players",
        "Таблица_7": "f7_sprav_gtin_vac",
        "Нанесения_ввод": "f1_manuf_import"
    }

    prefixes_pattern = "|".join(re.escape(k) for k in MAPPING)

    processed = skipped = failed = 0
    for i, zp in enumerate(zip_paths, 1):
        try:
            name = zp.name
            filename = zp.stem
            prefix_match = re.match(f"^({prefixes_pattern})", filename)

            if prefix_match:
                prefix = prefix_match.group(1)
                table_suffix = MAPPING[prefix]
            else:
                table_suffix = safe_ascii_name(filename)

            if not table_suffix:
                table_suffix = "tbl_" + re.sub(r"[^a-z0-9]", "", hex(abs(hash(filename)))[2:])[:8]

            table_name = f"ice.bronze.{table_suffix}"
            print(f"📦 Using table: {table_name}", flush=True)

            esc = name.replace("'", "''")
            already = spark.sql(
                f"SELECT 1 FROM ice.bronze.load_log WHERE zip_name='{esc}' LIMIT 1"
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

            if csv_path.stat().st_size == 0:
                print(f"[WARN  {i}/{total}] {name}: CSV file is empty → skip", flush=True)
                skipped += 1
                continue

            size_mb = round(csv_path.stat().st_size / (1024*1024), 2)
            print(f"[EXTRACT {i}/{total}] -> {csv_path.name} ({size_mb} MB)", flush=True)
            print(f"✅ [CHECK] File exists: {csv_path.exists()} — {csv_path}")
            print(f"🧭 [ABS PATH] {csv_path.absolute().as_posix()}")

            try:
                # Явное чтение с windows-1251 + permissive + пустые значения
                df = (
                    spark.read
                    .option("sep", CSV_SEP)
                    .option("header", CSV_HEADER)
                    .option("encoding", "windows-1251")
                    .option("mode", "PERMISSIVE")
                    .option("nullValue", "")
                    .option("emptyValue", "")
                    .csv(str(csv_path.absolute().as_posix()))
                )
                print(f"✅ CSV прочитан: {csv_path.name}")
            except Exception as e:
                print(f"❌ Ошибка при чтении CSV: {csv_path.name}")
                print(e)


            print(f"📦 Using table: {table_name}", flush=True)
            table_exists = spark.catalog.tableExists(table_name)

            if not table_exists:
                df.writeTo(table_name).create()
                print(f"👉 [CREATE] {table_name}", flush=True)

            df.writeTo(table_name).append()
            print(f"👉 [APPEND] {name} -> {table_name}", flush=True)

            (spark.createDataFrame([(name,)], ["zip_name"])
                .withColumn("loaded_at", current_timestamp())
                .writeTo("ice.bronze.load_log").append())

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

    logged = spark.sql("SELECT COUNT(*) AS c FROM ice.bronze.load_log").first()["c"]
    print("===== SUMMARY =====", flush=True)
    print(f"found      : {total}", flush=True)
    print(f"processed  : {processed}", flush=True)
    print(f"skipped    : {skipped}", flush=True)
    print(f"failed     : {failed}", flush=True)
    print(f"logged rows: {logged}", flush=True)
    print("===================", flush=True)
    spark.stop()
    print("✅ [DONE]", flush=True)
