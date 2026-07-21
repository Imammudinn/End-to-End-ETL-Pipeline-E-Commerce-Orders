"""
Mini ETL Orchestrator
Simulasi cara kerja Apache Airflow - bisa langsung dijalankan!
Konsep: task dependency, retry, logging, validation gate
"""
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import pandas as pd
import numpy as np
import time
import os
from datetime import datetime

# === CONFIG ===
INPUT_FILE = 'raw_orders.csv'
OUTPUT_FILE = 'orders_clean.csv'
REPORT_FILE = 'summary_report.csv'
LOG_FILE = 'pipeline_log.txt'
MAX_RETRIES = 3

# === LOGGER ===
def log(task_name, status, message=""):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{timestamp}] [{task_name}] [{status}] {message}"
    print(entry)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(entry + '\n')

# === TASK RUNNER (simulates Airflow's task execution) ===
def run_task(task_name, task_func, retries=MAX_RETRIES):
    """
    Jalankan sebuah task dengan retry logic.
    Sama seperti Airflow: jika gagal, coba ulang sampai max retries.
    """
    for attempt in range(1, retries + 1):
        try:
            log(task_name, "RUNNING", f"Attempt {attempt}/{retries}")
            start = time.time()
            result = task_func()
            duration = round(time.time() - start, 2)
            log(task_name, "SUCCESS", f"Selesai dalam {duration}s")
            return result
        except Exception as e:
            log(task_name, "FAILED", f"Error: {e}")
            if attempt < retries:
                wait = attempt * 2  # exponential backoff
                log(task_name, "RETRY", f"Menunggu {wait}s sebelum retry...")
                time.sleep(wait)
            else:
                log(task_name, "FATAL", f"Gagal setelah {retries} percobaan!")
                raise

# ============================================
# TASK DEFINITIONS (setiap fungsi = 1 task)
# ============================================

def task_extract():
    """Task 1: Baca data mentah dari sumber"""
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"{INPUT_FILE} tidak ditemukan!")
    df = pd.read_csv(INPUT_FILE)
    log("extract", "INFO", f"Loaded {len(df)} baris dari {INPUT_FILE}")
    return df

def task_transform(df):
    """Task 2: Bersihkan dan transformasi data"""
    before = len(df)
    # Hapus duplikat
    df = df.drop_duplicates()
    log("transform", "INFO", f"Hapus {before - len(df)} duplikat")
    # Hapus harga negatif
    neg_count = (df['total_harga'] < 0).sum()
    df = df[df['total_harga'] >= 0]
    log("transform", "INFO", f"Hapus {neg_count} harga negatif")
    # Isi missing values
    df['customer_email'] = df['customer_email'].fillna('unknown@placeholder.com')
    df['total_harga'] = df['total_harga'].fillna(df['total_harga'].median())
    # Standarkan format
    df['tanggal_order'] = pd.to_datetime(df['tanggal_order'], format='mixed')
    df['kota'] = df['kota'].str.strip().str.title()
    df['channel'] = df['channel'].str.strip().str.lower().str.replace(' ', '_')
    # Kolom baru
    df['bulan'] = df['tanggal_order'].dt.month_name()
    df['kategori_harga'] = np.where(
        df['total_harga'] < 500000, 'kecil',
        np.where(df['total_harga'] <= 2000000, 'sedang', 'besar')
    )
    log("transform", "INFO", f"Output: {len(df)} baris bersih")
    return df

def task_validate(df):
    """Task 3: Validasi kualitas data - GATE sebelum load"""
    checks = {
        'zero_duplicates': df.duplicated().sum() == 0,
        'zero_nulls': df.isnull().sum().sum() == 0,
        'zero_negative_price': (df['total_harga'] < 0).sum() == 0,
        'datetime_type': str(df['tanggal_order'].dtype).startswith('datetime'),
    }
    failed = [k for k, v in checks.items() if not v]
    if failed:
        raise ValueError(f"VALIDASI GAGAL: {failed}")
    log("validate", "INFO", f"Semua {len(checks)} check PASSED ✅")
    return df

def task_load(df):
    """Task 4: Simpan ke 'warehouse' (simulasi: CSV file)"""
    df.to_csv(OUTPUT_FILE, index=False)
    log("load", "INFO", f"Data disimpan ke {OUTPUT_FILE} ({len(df)} baris)")
    return df

def task_report(df):
    """Task 5: Generate summary report"""
    summary = df.groupby('kategori_harga').agg(
        total_orders=('order_id', 'count'),
        total_revenue=('total_harga', 'sum'),
        avg_revenue=('total_harga', 'mean')
    ).round(0)
    summary.to_csv(REPORT_FILE)
    log("report", "INFO", f"Report disimpan ke {REPORT_FILE}")
    return summary

def task_notify(summary):
    """Task 6: Kirim notifikasi (simulasi: print)"""
    total_orders = summary['total_orders'].sum()
    total_revenue = summary['total_revenue'].sum()
    message = (
        f"\n{'='*50}\n"
        f"📧 PIPELINE NOTIFICATION\n"
        f"{'='*50}\n"
        f"Status: ✅ SUCCESS\n"
        f"Waktu: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Total orders diproses: {int(total_orders)}\n"
        f"Total revenue: Rp {int(total_revenue):,}\n"
        f"Output: {OUTPUT_FILE}, {REPORT_FILE}\n"
        f"{'='*50}"
    )
    print(message)
    log("notify", "INFO", "Notifikasi terkirim")

# ============================================
# DAG DEFINITION - urutan eksekusi task
# ============================================

def run_pipeline():
    """
    DAG: etl_ecommerce_daily
    Schedule: manual (di Airflow: '0 6 * * *' = tiap hari jam 6)

    Task Flow:
      extract >> transform >> validate >> load >> report >> notify

    Jika validate gagal, pipeline BERHENTI - data kotor tidak di-load.
    """
    print("\n" + "="*50)
    print("🚀 STARTING ETL PIPELINE")
    print("="*50 + "\n")

    # Clear log
    open(LOG_FILE, 'w', encoding='utf-8').close()

    start_time = time.time()

    try:
        # Task 1: Extract
        df = run_task("extract", task_extract)

        # Task 2: Transform (depends on extract)
        df = run_task("transform", lambda: task_transform(df))

        # Task 3: Validate (depends on transform)
        # INI ADALAH GATE - jika gagal, load TIDAK dijalankan
        df = run_task("validate", lambda: task_validate(df))

        # Task 4: Load (depends on validate)
        df = run_task("load", lambda: task_load(df))

        # Task 5: Report (depends on load)
        summary = run_task("report", lambda: task_report(df))

        # Task 6: Notify (depends on report)
        run_task("notify", lambda: task_notify(summary))

        total_time = round(time.time() - start_time, 2)
        log("pipeline", "COMPLETED", f"Total waktu: {total_time}s")

    except Exception as e:
        log("pipeline", "FAILED", f"Pipeline gagal: {e}")
        print(f"\n❌ PIPELINE GAGAL: {e}")
        print(f"Lihat {LOG_FILE} untuk detail.")

if __name__ == '__main__':
    run_pipeline()