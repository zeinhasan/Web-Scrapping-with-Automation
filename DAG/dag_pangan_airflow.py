# dag_pangan_harga_to_bq.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import pandas as pd
import requests
from datetime import datetime
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig
from google.oauth2 import service_account

# Default arguments
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': 10,  # seconds
}

# Instantiate the DAG
with DAG(
    dag_id='fetch_pangan_harga_and_load_to_bigquery',
    default_args=default_args,
    description='Fetch pangan harga bulanan and load to BigQuery',
    schedule_interval='@daily',  # or your custom schedule
    start_date=datetime(2025, 4, 27),
    catchup=False,
) as dag:

    def fetch_and_clean():
        # --- YOUR API CALLING FUNCTION ---
        def ambil_data_pangan_bulanan(start_year, end_year=None, period_date=None, level_harga_id=3, province_id=""):
            if end_year is None:
                end_year = datetime.now().year
            if period_date is None:
                today = datetime.now().strftime("%d/%m/%Y")
                period_date = f"{today} - {today}"

            url = (
                f"https://api-panelhargav2.badanpangan.go.id/api/front/harga-pangan-bulanan-v2"
                f"?start_year={start_year}&end_year={end_year}&period_date={period_date}"
                f"&province_id={province_id}&level_harga_id={level_harga_id}"
            )
            response = requests.get(url)
            if response.status_code != 200:
                print(f"Request failed. Status: {response.status_code}")
                return {}
            try:
                data_json = response.json()
                return data_json
            except Exception as e:
                print("Failed to parse JSON:", e)
                return {}

        # --- FETCH DATA ---
        data = ambil_data_pangan_bulanan("2023")
        current_year = datetime.now().year
        komoditas_data = data['data'][str(current_year)]

        # --- CLEAN DATA ---
        data_list = []
        for komoditas in komoditas_data:
            komoditas_id = komoditas['Komoditas_id']
            komoditas_nama = komoditas['Komoditas']
            satuan = komoditas['today_province_price']['satuan']
            harga_tertinggi = komoditas['today_province_price']['hargatertinggi']
            harga_terendah = komoditas['today_province_price']['hargaterendah']
            harga_ratarata = komoditas['today_province_price']['hargaratarata']
            provinsi_tertinggi = komoditas['today_province_price']['provinsitertinggi']
            provinsi_terendah = komoditas['today_province_price']['provinsiterendah']

            harga_awal_waspada = None
            harga_akhir_waspada = None
            if 'setting_harga' in komoditas['today_province_price']:
                setting_harga = komoditas['today_province_price']['setting_harga']
                if len(setting_harga) > 0:
                    harga_awal_waspada = setting_harga[0]['harga_provinsi']
                if len(setting_harga) > 1:
                    harga_akhir_waspada = setting_harga[1]['harga_provinsi']

            data_list.append({
                "Komoditas_ID": komoditas_id,
                "Komoditas": komoditas_nama,
                "Satuan": satuan,
                "Harga_Tertinggi": harga_tertinggi,
                "Harga_Terendah": harga_terendah,
                "Harga_Rata_Rata": harga_ratarata,
                "Provinsi_Tertinggi": provinsi_tertinggi,
                "Provinsi_Terendah": provinsi_terendah,
                "Harga_Awal_Waspada": harga_awal_waspada,
                "Harga_Akhir_Waspada": harga_akhir_waspada,
                "Tanggal": datetime.now()
            })

        # Convert to DataFrame
        df = pd.DataFrame(data_list)

        # Save temporarily
        df.to_csv('/tmp/data_pangan.csv', index=False)

    def load_to_bigquery():
        # Load CSV
        df = pd.read_csv('/tmp/data_pangan.csv')

        credentials = service_account.Credentials.from_service_account_info({
        })

        bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        table_ref = "IHMSI.HargaKomoditasPangan"  # <<--- Ganti nama tabel di sini
        job_config = LoadJobConfig(
            autodetect=True,
            write_disposition="WRITE_APPEND"  # <<--- IMPORTANT: Append, not truncate
        )
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Data loaded successfully to {table_ref}")

    # Define the tasks
    t1 = PythonOperator(
        task_id='fetch_and_clean',
        python_callable=fetch_and_clean
    )

    t2 = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery
    )

    # Set task dependencies
    t1 >> t2
