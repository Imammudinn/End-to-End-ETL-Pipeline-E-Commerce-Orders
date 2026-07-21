import pandas as pd
import numpy as np

# === EXTRACT ===
# Baca data dari "sumber" (simulasi: file CSV)
orders = pd.read_csv('raw_orders.csv')
products = pd.read_csv('raw_products.csv')

# Inspeksi awal
print("=== ORDERS INFO ===")
print(f"Jumlah baris: {len(orders)}")
print(f"Kolom: {list(orders.columns)}")
print(orders.info())
print()
print("=== CEK MASALAH ===")
print(f"Duplikasi: {orders.duplicated().sum()}")
print(f"Missing values:")
print(orders.isnull().sum())
print(f"\nHarga negatif: {(orders['total_harga'] < 0).sum()}")
print(f"\nChannel unik: {orders['channel'].unique()}")
print(f"Kota unik: {orders['kota'].unique()}")


# === TRANSFORM ===

# 2a. Hapus duplikasi
print(f"Sebelum: {len(orders)} baris")
orders = orders.drop_duplicates()
print(f"Setelah hapus duplikat: {len(orders)} baris")

# 2b. Hapus baris dengan harga negatif (data error)
orders = orders[orders['total_harga'] >= 0]
print(f"Setelah hapus harga negatif: {len(orders)} baris")

# 2c. Isi missing values
orders['customer_email'] = orders['customer_email'].fillna('unknown@placeholder.com')
median_harga = orders['total_harga'].median()
orders['total_harga'] = orders['total_harga'].fillna(median_harga)
print(f"Missing values setelah fillna: {orders.isnull().sum().sum()}")

# 2d. Standarkan format tanggal
orders['tanggal_order'] = pd.to_datetime(orders['tanggal_order'], format='mixed')
print(f"Tipe tanggal: {orders['tanggal_order'].dtype}")

# 2e. Standarkan teks (lowercase lalu title case)
orders['kota'] = orders['kota'].str.strip().str.title()
orders['channel'] = orders['channel'].str.strip().str.lower().str.replace(' ', '_')
print(f"Channel setelah standarisasi: {orders['channel'].unique()}")
print(f"Kota setelah standarisasi: {orders['kota'].unique()}")

# 2f. Buat kolom baru: bulan dan kategori harga
orders['bulan'] = orders['tanggal_order'].dt.month_name()
orders['kategori_harga'] = np.where(
    orders['total_harga'] < 500000, 'kecil',
    np.where(orders['total_harga'] <= 2000000, 'sedang', 'besar')
)

print(f"\nDistribusi kategori harga:")
print(orders['kategori_harga'].value_counts())



# === VALIDATE ===
print("=== VALIDASI DATA BERSIH ===")

checks = {
    'Tidak ada duplikat': orders.duplicated().sum() == 0,
    'Tidak ada missing value': orders.isnull().sum().sum() == 0,
    'Tidak ada harga negatif': (orders['total_harga'] < 0).sum() == 0,
    'Tanggal tipe datetime': str(orders['tanggal_order'].dtype) == 'datetime64[us]',
    'Channel konsisten': len(orders['channel'].unique()) <= 3,
}

for check, passed in checks.items():
    status = 'OK' if passed else 'GAGAL'
    print(f"  [{status}] {check}")

all_passed = all(checks.values())
print(f"\nHasil: {'SEMUA LOLOS' if all_passed else 'ADA YANG GAGAL'}")


# === LOAD ===
# Di dunia nyata: load ke BigQuery / data warehouse
# Di exercise ini: simpan ke CSV bersih

orders_clean = orders[[
    'order_id', 'product_id', 'product_name', 'kategori',
    'quantity', 'total_harga', 'tanggal_order', 'kota',
    'channel', 'status', 'customer_email',
    'bulan', 'kategori_harga'
]]

orders_clean.to_csv('orders_clean.csv', index=False)
print(f"Data bersih disimpan: orders_clean.csv ({len(orders_clean)} baris)")

# Buat summary report
summary = orders_clean.groupby('kategori').agg(
    total_orders=('order_id', 'count'),
    total_revenue=('total_harga', 'sum'),
    avg_revenue=('total_harga', 'mean')
).round(0)

print("\n=== SUMMARY PER KATEGORI ===")
print(summary)

summary.to_csv('summary_report.csv')
print("\nSummary disimpan: summary_report.csv")