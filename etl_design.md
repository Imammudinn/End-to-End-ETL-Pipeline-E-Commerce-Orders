# ETL Pipeline Design: E-Commerce Orders

## 1. Overview

Pipeline ini memproses data transaksi e-commerce harian dari berbagai channel penjualan (website, marketplace, mobile app). Data mentah memiliki berbagai masalah kualitas yang perlu ditangani sebelum bisa digunakan untuk analisis bisnis.

**Tujuan:**
- Membersihkan dan menstandarkan data transaksi dari berbagai sumber
- Memvalidasi kualitas data sebelum disimpan ke warehouse
- Menghasilkan summary report per kategori untuk kebutuhan bisnis
- Mengotomatiskan proses ini agar berjalan harian tanpa intervensi manual

**Tech Stack:**
| Tool | Fungsi |
|---|---|
| Python 3.11 | Bahasa pemrograman utama |
| Pandas | Data manipulation & transformation |
| NumPy | Operasi numerik (kategorisasi harga) |
| Apache Airflow | Orchestration & scheduling |
| Docker | Container untuk Airflow |

---

## 2. Extract

### Sumber Data
- **File:** `raw_orders.csv` — Data transaksi e-commerce
- **File:** `raw_products.csv` — Master data produk (referensi)
- **Format:** CSV (Comma-Separated Values)
- **Volume:** ~130 baris per batch (simulasi data harian)
- **Frekuensi:** Harian (setiap hari jam 6 pagi WIB)

### Kolom Data Mentah (`raw_orders.csv`)
| Kolom | Tipe | Contoh |
|---|---|---|
| `order_id` | String | ORD-10056 |
| `product_id` | String | P005 |
| `product_name` | String | Monitor Samsung 24" |
| `kategori` | String | Elektronik, Furniture |
| `quantity` | Integer | 1–5 |
| `total_harga` | Float | 450000.0 |
| `tanggal_order` | String (mixed format!) | 2024-05-18, Jun 03 2024, 03/06/2024 |
| `kota` | String | SURABAYA, surabaya, Surabaya |
| `channel` | String | Website, MARKETPLACE, mobile_app |
| `status` | String | completed, pending, shipped, cancelled |
| `customer_email` | String (nullable) | customer1@email.com |

### Masalah yang Ditemukan di Data Mentah
| Masalah | Jumlah | Contoh |
|---|---|---|
| Baris duplikat | 10 baris | Baris identik yang muncul 2x |
| Missing values (`customer_email`) | ~15 baris | Kolom email kosong |
| Missing values (`total_harga`) | ~5 baris | Harga tidak terisi |
| Harga negatif | 5 baris | total_harga = -500000 |
| Format tanggal tidak konsisten | Semua baris | 4+ format berbeda |
| Inkonsistensi huruf besar/kecil | Banyak | SURABAYA vs surabaya vs Surabaya |
| Inkonsistensi channel naming | Banyak | Website vs MARKETPLACE vs mobile_app |

---

## 3. Transform

### Langkah-langkah Transformasi

#### Langkah 1: Hapus Duplikasi
- **Apa:** Menghapus baris yang 100% identik menggunakan `drop_duplicates()`
- **Kenapa:** Duplikasi bisa terjadi karena double-submit order atau error saat data transfer. Jika tidak dihapus, revenue dan order count akan di-inflate.
- **Hasil:** 130 → 120 baris (10 duplikat dihapus)

#### Langkah 2: Hapus Harga Negatif
- **Apa:** Filter out baris dengan `total_harga < 0`
- **Kenapa:** Harga negatif adalah data error/korup. Tidak ada skenario bisnis yang valid untuk transaksi dengan harga negatif (refund memiliki field terpisah di production).
- **Hasil:** 120 → 115 baris (5 harga negatif dihapus)

#### Langkah 3: Isi Missing Values
- **Apa:**
  - `customer_email` yang kosong → diisi `'unknown@placeholder.com'`
  - `total_harga` yang kosong → diisi dengan **median** harga
- **Kenapa:**
  - Email: Placeholder agar query downstream tidak error karena NULL. Guest checkout tanpa email memang bisa terjadi.
  - Harga: Median dipilih daripada mean karena tidak terpengaruh outlier (ada laptop seharga 15 juta yang akan bias rata-rata).
- **Hasil:** 0 missing values tersisa

#### Langkah 4: Standarkan Format Tanggal
- **Apa:** Konversi semua format tanggal ke `datetime64` menggunakan `pd.to_datetime(format='mixed')`
- **Kenapa:** Data mentah punya 4+ format tanggal berbeda (`2024-05-18`, `Jun 03, 2024`, `03/06/2024`, `2024-06-28 10:51:00`). Tanpa standarisasi, operasi time-series (groupby bulan, filter tanggal) tidak bisa dilakukan.
- **Hasil:** Semua tanggal menjadi `datetime64` yang konsisten

#### Langkah 5: Standarkan Teks
- **Apa:**
  - `kota`: strip whitespace → Title Case (contoh: `SURABAYA` → `Surabaya`)
  - `channel`: strip → lowercase → replace spasi dengan underscore (contoh: `Website` → `website`)
- **Kenapa:** Inkonsistensi teks akan menyebabkan groupby/aggregasi pecah menjadi beberapa grup yang seharusnya satu. Contoh: tanpa standarisasi, "SURABAYA", "surabaya", "Surabaya" dihitung sebagai 3 kota berbeda.
- **Hasil:** Kota dan channel konsisten

#### Langkah 6: Feature Engineering
- **Apa:** Buat 2 kolom baru:
  - `bulan`: Nama bulan dari tanggal order (May, June, July)
  - `kategori_harga`: Segmentasi berdasarkan harga (kecil < 500rb, sedang 500rb-2jt, besar > 2jt)
- **Kenapa:** Kolom turunan ini mempermudah analisis tanpa perlu transformasi ulang setiap query. Segmentasi harga membantu tim bisnis melihat distribusi transaksi per tier.
- **Hasil:** 2 kolom baru ditambahkan

---

## 4. Load

### Tujuan Penyimpanan
- **Primary Output:** `orders_clean.csv` — Data bersih yang siap untuk analisis
- **Secondary Output:** `summary_report.csv` — Aggregated report per kategori harga
- **Format:** CSV (simulasi dari data warehouse)

### Kolom Output (`orders_clean.csv`)
13 kolom: `order_id`, `product_id`, `product_name`, `kategori`, `quantity`, `total_harga`, `tanggal_order`, `kota`, `channel`, `status`, `customer_email`, `bulan`, `kategori_harga`

### Summary Report (`summary_report.csv`)
| Kategori | Total Orders | Total Revenue | Avg Revenue |
|---|---|---|---|
| Elektronik | 81 | Rp 435.180.000 | Rp 5.372.593 |
| Furniture | 29 | Rp 127.350.000 | Rp 4.391.379 |

### Production Path (Next Steps)
Di environment production, output akan di-load ke:
- **Google BigQuery** sebagai data warehouse utama
- **PostgreSQL** sebagai operational database untuk dashboard
- **Google Cloud Storage** sebagai data lake untuk raw backup

---

## 5. Orchestration

### Tool: Apache Airflow
- **DAG Name:** `etl_ecommerce_daily`
- **Schedule:** `0 6 * * *` (setiap hari jam 06:00 WIB)
- **Catchup:** False (tidak menjalankan batch yang terlewat)
- **Owner:** `data-engineering-team`

### DAG Flow

```
start >> extract_orders >> transform_and_clean >> validate_quality >> load_to_warehouse >> generate_report >> end
```

```
┌───────┐    ┌─────────┐    ┌───────────┐    ┌──────────┐    ┌──────┐    ┌────────┐    ┌─────┐
│ START │───▶│ EXTRACT │───▶│ TRANSFORM │───▶│ VALIDATE │───▶│ LOAD │───▶│ REPORT │───▶│ END │
└───────┘    └─────────┘    └───────────┘    └──────────┘    └──────┘    └────────┘    └─────┘
                                                  │
                                                  │ GAGAL?
                                                  ▼
                                             ❌ PIPELINE
                                                STOP!
```

### Task Details
| Task ID | Fungsi | Retry |
|---|---|---|
| `start` | EmptyOperator — titik awal DAG | - |
| `extract_orders` | Baca `raw_orders.csv`, simpan ke temp file | 2x |
| `transform_and_clean` | 6 langkah transformasi | 2x |
| `validate_quality` | 4 data quality checks — **GATE** | 2x |
| `load_to_warehouse` | Simpan ke `orders_clean.csv` | 2x |
| `generate_report` | Buat summary + cleanup temp files | 2x |
| `end` | EmptyOperator — titik akhir DAG | - |

### Data Passing antar Task
Data di-pass antar task menggunakan **file-based approach** (bukan XCom untuk data besar):
1. `extract` simpan ke `_temp_extracted.csv`
2. `transform` baca temp, simpan ke `_temp_transformed.csv`
3. `validate` baca temp, return path
4. `load` baca temp, simpan ke file final
5. `report` baca file final, cleanup temp files

---

## 6. Error Handling

### Skenario 1: File Sumber Tidak Ditemukan
- **Penyebab:** File `raw_orders.csv` belum di-upload atau path salah
- **Handling:** `FileNotFoundError` di-raise → task gagal → Airflow retry 2x
- **Impact:** Pipeline berhenti total — tidak ada data yang diproses

### Skenario 2: Validasi Data Gagal
- **Penyebab:** Transformasi tidak berhasil membersihkan semua masalah
- **Handling:** `ValueError` di-raise dengan detail check mana yang gagal → **pipeline STOP**
- **Impact:** Data kotor TIDAK di-load ke warehouse — ini adalah fitur keamanan (**validation gate**)
- **Kenapa penting:** Lebih baik tidak ada data baru daripada data kotor masuk ke production

### Skenario 3: Task Individual Gagal (Generic)
- **Penyebab:** Error tak terduga (memory, disk full, library error)
- **Handling:** Airflow retry otomatis 2x dengan delay 1 menit antar retry
- **Impact:** Jika masih gagal setelah 2 retry → task marked FAILED → seluruh downstream task di-skip

### Skenario 4: Schema Berubah
- **Penyebab:** Sumber data menambah/menghapus kolom
- **Handling:** `KeyError` saat akses kolom → task gagal → retry (kemungkinan tetap gagal)
- **Solusi:** Alert ke tim → manual investigation → update script
- **Next step (production):** Tambahkan schema validation di step extract

---

## 7. Monitoring

### Bagaimana Cara Tahu Pipeline Sukses?
1. **Airflow UI** — Semua task berwarna hijau (success) di DAG view
2. **Log file** (`pipeline_log.txt`) — Entry `[pipeline] [COMPLETED]` di baris terakhir
3. **Output files exist** — `orders_clean.csv` dan `summary_report.csv` ter-update dengan timestamp hari ini
4. **Notification task** — Print summary dengan total orders dan revenue yang diproses

### Bagaimana Cara Tahu Data Berkualitas?
1. **Validation gate** — 5 automated checks harus PASS sebelum data di-load:
   - `zero_duplicates`: Tidak ada baris duplikat
   - `zero_nulls`: Tidak ada missing values
   - `zero_negative_price`: Tidak ada harga negatif
   - `datetime_type`: Kolom tanggal bertipe datetime (bukan string)
   - `channel_consistent`: Channel konsisten
2. **Row count monitoring** — Cek apakah jumlah baris output masuk akal (tidak terlalu sedikit/banyak)
3. **Revenue sanity check** — Total revenue per run dalam range yang wajar

