"""
Airflow DAG: ETL E-Commerce Orders
Pipeline harian untuk membersihkan dan memproses data transaksi e-commerce.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import pandas as pd
import numpy as np
import os

# === PATH CONFIG (di dalam Docker container) ===
DATA_DIR = '/opt/airflow/data'
INPUT_FILE = os.path.join(DATA_DIR, 'raw_orders.csv')
PRODUCTS_FILE = os.path.join(DATA_DIR, 'raw_products.csv')
OUTPUT_FILE = os.path.join(DATA_DIR, 'orders_clean.csv')
REPORT_FILE = os.path.join(DATA_DIR, 'summary_report.csv')


# ============================================
# TASK FUNCTIONS
# ============================================

def extract_from_source(**kwargs):
    """Task: Baca data mentah dari CSV"""
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"{INPUT_FILE} tidak ditemukan!")

    df = pd.read_csv(INPUT_FILE)
    print(f"[EXTRACT] Loaded {len(df)} baris dari {INPUT_FILE}")
    print(f"[EXTRACT] Kolom: {list(df.columns)}")
    print(f"[EXTRACT] Duplikasi: {df.duplicated().sum()}")
    print(f"[EXTRACT] Missing values:\n{df.isnull().sum()}")

    # Simpan ke file temporary agar bisa dibaca task berikutnya
    temp_path = os.path.join(DATA_DIR, '_temp_extracted.csv')
    df.to_csv(temp_path, index=False)
    return temp_path


def transform_data(**kwargs):
    """Task: Bersihkan dan transformasi data"""
    ti = kwargs['ti']
    temp_path = ti.xcom_pull(task_ids='extract_orders')
    df = pd.read_csv(temp_path)

    before = len(df)

    # Hapus duplikat
    df = df.drop_duplicates()
    print(f"[TRANSFORM] Hapus {before - len(df)} duplikat")

    # Hapus harga negatif
    neg_count = (df['total_harga'] < 0).sum()
    df = df[df['total_harga'] >= 0]
    print(f"[TRANSFORM] Hapus {neg_count} harga negatif")

    # Isi missing values
    df['customer_email'] = df['customer_email'].fillna('unknown@placeholder.com')
    df['total_harga'] = df['total_harga'].fillna(df['total_harga'].median())

    # Standarkan format tanggal
    df['tanggal_order'] = pd.to_datetime(df['tanggal_order'], format='mixed')

    # Standarkan teks
    df['kota'] = df['kota'].str.strip().str.title()
    df['channel'] = df['channel'].str.strip().str.lower().str.replace(' ', '_')

    # Kolom baru
    df['bulan'] = df['tanggal_order'].dt.month_name()
    df['kategori_harga'] = np.where(
        df['total_harga'] < 500000, 'kecil',
        np.where(df['total_harga'] <= 2000000, 'sedang', 'besar')
    )

    print(f"[TRANSFORM] Output: {len(df)} baris bersih")

    temp_path = os.path.join(DATA_DIR, '_temp_transformed.csv')
    df.to_csv(temp_path, index=False)
    return temp_path


def validate_data(**kwargs):
    """Task: Validasi kualitas data - GATE sebelum load"""
    ti = kwargs['ti']
    temp_path = ti.xcom_pull(task_ids='transform_and_clean')
    df = pd.read_csv(temp_path)

    # Parse tanggal kembali untuk validasi tipe
    df['tanggal_order'] = pd.to_datetime(df['tanggal_order'])

    checks = {
        'zero_duplicates': df.duplicated().sum() == 0,
        'zero_nulls': df.isnull().sum().sum() == 0,
        'zero_negative_price': (df['total_harga'] < 0).sum() == 0,
        'datetime_type': str(df['tanggal_order'].dtype).startswith('datetime'),
    }

    for check, passed in checks.items():
        status = 'PASS ✅' if passed else 'FAIL ❌'
        print(f"[VALIDATE] {check}: {status}")

    failed = [k for k, v in checks.items() if not v]
    if failed:
        raise ValueError(f"VALIDASI GAGAL: {failed}")

    print(f"[VALIDATE] Semua {len(checks)} check PASSED ✅")
    return temp_path


def load_to_warehouse(**kwargs):
    """Task: Simpan data bersih ke output file"""
    ti = kwargs['ti']
    temp_path = ti.xcom_pull(task_ids='validate_quality')
    df = pd.read_csv(temp_path)

    # Parse tanggal kembali
    df['tanggal_order'] = pd.to_datetime(df['tanggal_order'])

    # Pilih kolom final
    output_cols = [
        'order_id', 'product_id', 'product_name', 'kategori',
        'quantity', 'total_harga', 'tanggal_order', 'kota',
        'channel', 'status', 'customer_email',
        'bulan', 'kategori_harga'
    ]
    df_clean = df[output_cols]
    df_clean.to_csv(OUTPUT_FILE, index=False)
    print(f"[LOAD] Data disimpan ke {OUTPUT_FILE} ({len(df_clean)} baris)")
    return OUTPUT_FILE


def generate_summary(**kwargs):
    """Task: Generate summary report per kategori harga"""
    df = pd.read_csv(OUTPUT_FILE)

    summary = df.groupby('kategori_harga').agg(
        total_orders=('order_id', 'count'),
        total_revenue=('total_harga', 'sum'),
        avg_revenue=('total_harga', 'mean')
    ).round(0)

    summary.to_csv(REPORT_FILE)
    print(f"[REPORT] Summary disimpan ke {REPORT_FILE}")
    print(f"[REPORT]\n{summary}")

    # Cleanup temp files
    for f in ['_temp_extracted.csv', '_temp_transformed.csv']:
        fpath = os.path.join(DATA_DIR, f)
        if os.path.exists(fpath):
            os.remove(fpath)
            print(f"[REPORT] Cleanup: {f} dihapus")

    return REPORT_FILE


# ============================================
# DEFAULT ARGS
# ============================================

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}


# ============================================
# DAG DEFINITION
# ============================================

with DAG(
    dag_id='etl_ecommerce_daily',
    default_args=default_args,
    description='Daily ETL pipeline untuk data transaksi e-commerce',
    schedule='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'ecommerce', 'daily'],
) as dag:

    start = EmptyOperator(task_id='start')

    extract = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_from_source,
    )

    transform = PythonOperator(
        task_id='transform_and_clean',
        python_callable=transform_data,
    )

    validate = PythonOperator(
        task_id='validate_quality',
        python_callable=validate_data,
    )

    load = PythonOperator(
        task_id='load_to_warehouse',
        python_callable=load_to_warehouse,
    )

    report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_summary,
    )

    end = EmptyOperator(task_id='end')

    # === TASK DEPENDENCIES (DAG Flow) ===
    start >> extract >> transform >> validate >> load >> report >> end