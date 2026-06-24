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

## 🚕 NYC Taxi ETL → ELT (tahap eksplorasi)

Tahap awal sebelum migrasi ke BigQuery. Mulai dari ETL berbasis raw SQL, lalu bergeser ke pola ELT (transform di dalam warehouse via dbt) — tapi masih di atas PostgreSQL.

Model yang ada di sini: `stg_nyc_taxi_trips`, `fct_daily_revenue`, `fct_trips_incremental`, `fct_long_trip_revenue`, snapshot (`nyc_trips_snapshot`) dengan strategy timestamp.

**Versi production (full dbt + BigQuery) ada di [`nyc-taxi-dbt-pipeline`](https://github.com/NajmaFahmi/nyc-taxi-dbt-pipeline)** — lanjut ke sana untuk lihat hasil final, termasuk catatan migrasi PostgreSQL→BigQuery.

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
├── macros/                              # filter_valid_trips, calculate_duration_hours
├── models/
│   ├── staging/                         # stg_nyc_taxi_trips, stg_olist_orders
│   └── marts/                           # fct_daily_revenue, olist_fct_orders, dll
├── seeds/
├── snapshots/                           # nyc_trips_snapshot (timestamp strategy)
└── tests/                               # custom test: olist_assert_positive_payment_value
```
