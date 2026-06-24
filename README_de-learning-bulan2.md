# de-learning-bulan2 — dbt Learning Sandbox

Repo latihan dbt Bulan 2 (Week 1–3) dari roadmap Data Engineering pribadi. Berisi beberapa exercise dengan status berbeda — lihat tabel di bawah sebelum membaca kode.

## 📍 Status per Project

| Project | Status | Lokasi |
|---|---|---|
| **NYC Taxi ETL → ELT** | ✅ Graduated — dilanjutkan di repo terpisah dengan migrasi ke BigQuery | [`nyc-taxi-dbt-pipeline`](https://github.com/NajmaFahmi/nyc-taxi-dbt-pipeline) |
| **Olist E-commerce** | ✅ Final — tetap di repo ini | `models/staging/stg_olist_orders.sql`, `models/marts/olist_*` |
| **Airflow DAG practice** | 🧪 Exercise only, bukan production pipeline | `airflow_project/airflow_home/dags/` |

> Kalau kamu mencari pipeline NYC Taxi versi production (dbt + BigQuery), buka **[nyc-taxi-dbt-pipeline](https://github.com/NajmaFahmi/nyc-taxi-dbt-pipeline)**. Versi di repo ini adalah tahap awal eksplorasi sebelum migrasi.

---

## 🛒 Olist E-commerce — Star Schema

### Problem
Data order e-commerce mentah (Olist dataset) perlu ditransformasi jadi model analitik yang bisa jawab pertanyaan bisnis: revenue per kategori produk, performa per kota/state pelanggan, tren per hari/bulan, dan rate keterlambatan pengiriman.

### Architecture

```
raw.olist_orders_table (Postgres)
        │
        ▼
stg_olist_orders.sql   ← filter: hanya order 'delivered', payment_value not null
        │
        ├──► olist_dim_customer  (customer_id, city, state)
        ├──► olist_dim_product   (product_id, category)
        ├──► olist_dim_date      (date_id surrogate key, month, year, is_weekend)
        │
        ▼
olist_fct_orders.sql   ← fact table, join ke 3 dimension di atas
```

### Tech Stack
- dbt-core
- PostgreSQL (source: `latihan_de.public.olist_orders_table`)
- `dbt_utils` package — untuk `generate_surrogate_key()` di dim_date

### Key Decisions
- **Filter di staging layer, bukan di fact table** — `stg_olist_orders` hanya meloloskan order dengan status `delivered` dan `payment_value` tidak null. Keputusan ini supaya semua model marts hilir otomatis bersih tanpa perlu filter berulang di setiap fact/dim.
- **Surrogate key untuk `date_id`** — pakai `dbt_utils.generate_surrogate_key()` daripada raw date sebagai PK, supaya konsisten dengan konvensi dimensional modeling (surrogate key, bukan natural key).
- **Join dim_date pakai `DATE()` cast** — `order_purchase_timestamp` bertipe timestamp, sedangkan `full_date` di dim_date bertipe date. Awalnya join langsung tanpa cast menyebabkan `date_id` NULL untuk hampir semua row (silent failure — query jalan tanpa error, tapi data salah). Fix: `DATE(o.order_purchase_timestamp) = d.full_date`.

### Data Quality
- Test `unique` + `not_null` pada `order_id` dan `date_id` (fact table)
- Test custom (`olist_assert_positive_payment_value.sql`) — pastikan tidak ada `payment_value` negatif

### Hasil
- Fact table `olist_fct_orders` link ke 3 dimension table tanpa NULL pada foreign key (setelah fix join date)

---

## 🚕 NYC Taxi — Data Modeling (logic dibangun di sini)

> Repo ini berisi **semua logic transformasi** untuk NYC Taxi. Repo [`nyc-taxi-dbt-pipeline`](https://github.com/NajmaFahmi/nyc-taxi-dbt-pipeline) hanya memindahkan model yang sama ini dari PostgreSQL ke BigQuery — bukan menulis ulang logic. Untuk memahami *bagaimana* dan *kenapa* setiap model didesain, baca section ini.

### Architecture

```
raw.nyc_taxi_trips (Postgres, loaded via Airflow)
        │
        ▼
stg_nyc_taxi_trips.sql
   filter: passenger_count > 0, trip_distance > 0, fare_amount > 0
   derived: tip_percentage = tip_amount / fare_amount * 100
        │
        ├──► fct_daily_revenue        (agregasi harian, semua trip)
        ├──► fct_trips_incremental    (incremental load, per-trip)
        ├──► stg_long_trips → fct_long_trip_revenue   (agregasi harian, trip > 10 mil)
        ├──► nyc_dim_date             (dimension date, surrogate key)
        └──► nyc_trips_snapshot       (snapshot, strategy timestamp)
```

### Model breakdown

**`stg_nyc_taxi_trips`** — staging layer. Filter row tidak valid (passenger 0, distance 0, fare 0) dan hitung `tip_percentage` sebagai derived column. Semua model di bawah ini konsumsi dari staging ini, bukan dari raw table langsung — supaya filter validasi cukup ditulis sekali.

**`fct_daily_revenue`** — fact table agregasi per hari: total trip, total revenue, rata-rata jarak, rata-rata tip percentage. Materialized sebagai table biasa (full refresh tiap `dbt run`), karena volumenya kecil dan butuh akurasi penuh tiap kali jalan.

**`fct_trips_incremental`** — fact table per-trip (bukan agregat), `materialized='incremental'` dengan `unique_key='trip_id'`. Pakai `is_incremental()` macro built-in dbt — hanya insert row dengan `loaded_at` lebih baru dari max `loaded_at` yang sudah ada di tabel. Dipilih incremental (bukan table biasa) karena ini level granularity per-trip, yang volumenya akan terus bertambah — reprocess semua row tiap run tidak efisien.

**`stg_long_trips` → `fct_long_trip_revenue`** — segmentasi khusus trip jarak jauh (`trip_distance > 10` mil). Dipisah jadi staging tersendiri (bukan filter inline di fact table) supaya definisi "long trip" terpusat di satu tempat — kalau threshold berubah dari 10 mil ke angka lain, cuma satu file yang diedit.

**`nyc_dim_date`** — dimension date dengan surrogate key (`generate_surrogate_key`), plus kolom `day_of_week`, `month`, `year`, `is_weekend`. Dipakai untuk join di `nyc_fct_trips` (model star-schema, join trip ke date).

**`nyc_trips_snapshot`** — dbt snapshot dengan `strategy='timestamp'`, tracking perubahan row di source berdasarkan kolom `loaded_at`. Berguna kalau source data NYC taxi di-update/dikoreksi setelah load awal — snapshot menyimpan histori versi lama, bukan overwrite.

### Catatan jujur: macro tidak terpakai
Ada 2 macro custom — `filter_valid_trips(min_distance, min_fare)` dan `calculate_duration_hours(column_name)` — yang dibuat sebagai exercise Jinja/macro dbt, **tapi tidak dipanggil di model manapun**. Filter validasi di `stg_nyc_taxi_trips` ditulis manual, bukan via macro ini. Disimpan di repo sebagai bukti latihan macro, bukan bagian dari pipeline aktif.

### Lanjut ke versi production
Versi BigQuery (migrasi penuh, sama logic model di atas, beda warehouse) + catatan proses migrasi PostgreSQL→BigQuery ada di **[`nyc-taxi-dbt-pipeline`](https://github.com/NajmaFahmi/nyc-taxi-dbt-pipeline)**.

---

## 🧪 Airflow DAG Practice

Folder `airflow_project/airflow_home/dags/` berisi DAG latihan konsep Airflow, **bukan pipeline production**:
- `dag_nyc_taxi_etl.py` — DAG awal untuk load NYC taxi data
- `dag_branch_practice.py` — latihan branching (`BranchPythonOperator`)
- `dag_xcom_practice.py` — latihan passing data antar task via XCom

---

## ⚙️ Setup

```bash
git clone https://github.com/NajmaFahmi/de-learning-bulan2.git
cd de-learning-bulan2

python3 -m venv venv_de
source venv_de/bin/activate
pip install dbt-postgres

dbt deps        # install packages.yml (dbt_utils)
dbt seed
dbt run
dbt test
```

Memerlukan PostgreSQL lokal dengan database `latihan_de` (lihat [de-learning-bulan1](https://github.com/NajmaFahmi/de-learning-bulan1) untuk setup awal dataset).

---

## 📁 Struktur Folder

```
de-learning-bulan2/
├── airflow_project/airflow_home/dags/   # Airflow DAG practice
├── analyses/
├── macros/                              # filter_valid_trips, calculate_duration_hours (exercise, belum dipakai)
├── models/
│   ├── staging/
│   │   ├── stg_nyc_taxi_trips.sql
│   │   ├── stg_long_trips.sql
│   │   └── stg_olist_orders.sql
│   └── marts/
│       ├── fct_daily_revenue.sql
│       ├── fct_trips_incremental.sql
│       ├── fct_long_trip_revenue.sql
│       ├── nyc_dim_date.sql
│       ├── nyc_fct_trips.sql
│       ├── olist_dim_customer.sql
│       ├── olist_dim_product.sql
│       ├── olist_dim_date.sql
│       └── olist_fct_orders.sql
├── seeds/
├── snapshots/                           # nyc_trips_snapshot (timestamp strategy)
└── tests/                               # custom test: olist_assert_positive_payment_value
```
