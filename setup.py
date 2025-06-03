import duckdb
import pandas as pd
import os


def setup_database():
    # Connexion à la base DuckDB
    conn = duckdb.connect(database='veepee_ads.db', read_only=False)

    # Création des tables
    create_tables(conn)

    # Chargement des données
    load_data(conn)

    conn.close()


def create_tables(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_users (
        user_id VARCHAR,
        gender VARCHAR,
        status VARCHAR,
        is_optim BOOLEAN
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_campaigns (
        campaign_id VARCHAR,
        start_datetime TIMESTAMP,
        end_datetime TIMESTAMP,
        main_sector VARCHAR,
        main_sub_sector VARCHAR,
        brand_name VARCHAR,
        site_country_code VARCHAR
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_orders (
        user_id VARCHAR,
        id VARCHAR,
        campaign_id VARCHAR,
        order_id VARCHAR,
        product_id VARCHAR,
        order_creation_date DATE,
        product_booking_incl_vat DECIMAL(10,2),
        product_sold INTEGER,
        is_cancelled BOOLEAN,
        main_sector VARCHAR,
        main_sub_sector VARCHAR,
        brand_name VARCHAR,
        site_country_code VARCHAR
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_visits (
        user_id VARCHAR,
        campaign_id VARCHAR,
        visit_date DATE,
        banner_type VARCHAR,
        nb_events INTEGER,
        site_country_code VARCHAR
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_thematics_mapping (
        sector VARCHAR,
        sub_sector VARCHAR,
        theme VARCHAR
    )
    """)


def load_data(conn):
    data_dir = 'data'

    # === USERS ===
    users_df = pd.read_parquet(os.path.join(data_dir, 'users.parquet'))
    conn.register('users_df', users_df)
    conn.execute("INSERT INTO raw_users SELECT * FROM users_df")

    # === CAMPAIGNS ===
    campaigns_df = pd.read_parquet(os.path.join(data_dir, 'campaigns.parquet'))
    conn.register('campaigns_df', campaigns_df)
    conn.execute("INSERT INTO raw_campaigns SELECT * FROM campaigns_df")

    # === ORDERS ===
    orders_df = pd.read_parquet(os.path.join(data_dir, 'orders.parquet'))
    conn.register('orders_df', orders_df)
    conn.execute("INSERT INTO raw_orders SELECT * FROM orders_df")

    # === VISITS ===
    visits_df = pd.read_parquet(os.path.join(data_dir, 'visits.parquet'))
    conn.register('visits_df', visits_df)
    conn.execute("INSERT INTO raw_visits SELECT * FROM visits_df")

    # === THEMATIC MAPPING ===
    thematics_df = pd.read_csv(os.path.join(data_dir, 'thematics_mapping.csv'), sep=";", header=0)
    conn.register('thematics_df', thematics_df)
    conn.execute("INSERT INTO raw_thematics_mapping SELECT * FROM thematics_df")


if __name__ == '__main__':
    setup_database()
