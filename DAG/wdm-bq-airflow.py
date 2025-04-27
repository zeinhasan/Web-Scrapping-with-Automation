# dag_scrape_and_load.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta

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
    dag_id='scrape_worldometers_and_load_to_bigquery',
    default_args=default_args,
    description='Scrape agriculture data and load to BigQuery',
    schedule_interval='@daily',  # Trigger manually or set your own interval
    start_date=datetime(2025, 4, 27),
    catchup=False,
) as dag:

    def scrape_and_clean():
        BASE_URL = 'https://www.worldometers.info'
        main_link = "https://www.worldometers.info/food-agriculture/"
        page = requests.get(main_link)
        soup = BeautifulSoup(page.content, 'html.parser')

        # Get countries links
        countries = []
        for a_tag in soup.select('ul li a'):
            country_name = a_tag.text.strip()
            link_ref = a_tag.get('href')
            full_url = BASE_URL + link_ref
            countries.append((country_name, full_url))

        countries = [country for country in countries if '/food-agriculture/' in country[1]]
        countries = countries[2:]  # Remove first two

        # Function to scrape a country
        def get_country_soup(country_url):
            response = requests.get(country_url)
            return BeautifulSoup(response.text, 'html.parser')

        def scrape_country_data(country_name, country_url):
            try:
                country_soup = get_country_soup(country_url)

                undernourished = None
                undernourished_pct = None
                share_global_undernourished = None
                undernourishment_rank = None

                h2_undernourished = country_soup.find('h2', id='undernourished')
                if h2_undernourished:
                    next_elements = h2_undernourished.find_all_next()
                    found_text_2xl = False
                    found_text_xl_count = 0
                    for elem in next_elements:
                        if elem.name == 'strong':
                            classes = elem.get('class', [])
                            if 'text-2xl' in classes and not found_text_2xl:
                                undernourished = elem.text.strip()
                                found_text_2xl = True
                            elif 'text-xl' in classes:
                                if found_text_xl_count == 0:
                                    undernourished_pct = elem.text.strip()
                                elif found_text_xl_count == 1:
                                    share_global_undernourished = elem.text.strip()
                                found_text_xl_count += 1
                        elif elem.name == 'a':
                            classes = elem.get('class', [])
                            if 'font-bold!' in classes and 'text-xl' in classes:
                                undernourishment_rank = elem.text.strip()

                forest_area = None
                share_world_forests = None
                h2_forest = country_soup.find('h2', id='forest')
                if h2_forest:
                    next_elements_forest = h2_forest.find_all_next()
                    found_forest_area = False
                    for elem in next_elements_forest:
                        if elem.name == 'strong':
                            classes = elem.get('class', [])
                            if 'text-2xl' in classes and not found_forest_area:
                                forest_area = elem.text.strip()
                                found_forest_area = True
                            elif 'text-xl' in classes and share_world_forests is None:
                                share_world_forests = elem.text.strip()
                                break

                cropland = None
                share_world_croplands = None
                h2_cropland = country_soup.find('h2', id='cropland')
                if h2_cropland:
                    next_elements_cropland = h2_cropland.find_all_next()
                    found_cropland_area = False
                    for elem in next_elements_cropland:
                        if elem.name == 'strong':
                            classes = elem.get('class', [])
                            if 'text-2xl' in classes and not found_cropland_area:
                                cropland = elem.text.strip()
                                found_cropland_area = True
                            elif 'text-xl' in classes and share_world_croplands is None:
                                share_world_croplands = elem.text.strip()
                                break

                return {
                    'Country': country_name,
                    'Undernourished': undernourished,
                    'Undernourished %': undernourished_pct,
                    'Undernourishment Rank': undernourishment_rank,
                    'Share of Global Undernourished': share_global_undernourished,
                    'Forest Area': forest_area,
                    'Share of World Forests': share_world_forests,
                    'Cropland': cropland,
                    'Share of World Croplands': share_world_croplands
                }
            except Exception as e:
                print(f"Failed to scrape {country_name}: {e}")
                return None

        all_data = []
        for country_name, country_url in countries:
            print(f"Scraping {country_name}...")
            data = scrape_country_data(country_name, country_url)
            if data:
                all_data.append(data)
            time.sleep(5)  # Respect server load

        print("\nScraping finished!")

        df = pd.DataFrame(all_data)
        df.columns = [
            'Country',
            'Undernourished',
            'Undernourished_pct',
            'Undernourishment_Rank',
            'Share_of_Global_Undernourished',
            'Forest_Area',
            'Share_of_World_Forests',
            'Cropland',
            'Share_of_World_Croplands'
        ]

        # Data Cleaning
        df['Undernourished'] = df['Undernourished'].str.replace(',', '').astype(float)
        df['Undernourished_pct'] = df['Undernourished_pct'].str.replace('%', '').str.replace(',', '').astype(float) / 100
        df['Undernourishment_Rank'] = df['Undernourishment_Rank'].str.replace('#', '').astype(float)
        df['Share_of_Global_Undernourished'] = df['Share_of_Global_Undernourished'].str.replace('%', '').str.replace(',', '').astype(float) / 100
        df['Forest_Area'] = df['Forest_Area'].str.replace(',', '').astype(float)
        df['Share_of_World_Forests'] = df['Share_of_World_Forests'].str.replace('%', '').str.replace(',', '').astype(float) / 100
        df['Cropland'] = df['Cropland'].str.replace(',', '').astype(float)
        df['Share_of_World_Croplands'] = df['Share_of_World_Croplands'].str.replace('%', '').str.replace(',', '').astype(float) / 100

        # Save to CSV temporarily to share between tasks
        df.to_csv('/tmp/scraped_data.csv', index=False)

    def load_to_bigquery():
        # Load CSV
        df = pd.read_csv('/tmp/scraped_data.csv')

        credentials = service_account.Credentials.from_service_account_info({})

        bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        table_ref = "IHMSI.WDM_Agriculture"
        job_config = LoadJobConfig(
            autodetect=True,
            write_disposition="WRITE_TRUNCATE"
        )
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Data loaded successfully to {table_ref}")

    # Define the tasks
    t1 = PythonOperator(
        task_id='scrape_and_clean',
        python_callable=scrape_and_clean
    )

    t2 = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery
    )

    # Set task dependencies
    t1 >> t2