from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import (
    MetaData, Table, Column, String, Integer, Float, DateTime,
    UniqueConstraint, inspect
)
def create_table(**kwargs):
    """
    Create the alt_users_churn table in the destination database if it doesn't exist.
    """
    table_name = kwargs.get('table_name', 'alt_users_churn')
    hook = PostgresHook('destination_db')
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData()
    
    # Check if table already exists
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        Table(
            table_name,
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('customer_id', String),
            Column('begin_date', DateTime),
            Column('end_date', DateTime),
            Column('type', String),
            Column('paperless_billing', String),
            Column('payment_method', String),
            Column('monthly_charges', Float),
            Column('total_charges', Float),
            Column('internet_service', String),
            Column('online_security', String),
            Column('online_backup', String),
            Column('device_protection', String),
            Column('tech_support', String),
            Column('streaming_tv', String),
            Column('streaming_movies', String),
            Column('gender', String),
            Column('senior_citizen', Integer),
            Column('partner', String),
            Column('dependents', String),
            Column('multiple_lines', String),
            Column('target', Integer),
            UniqueConstraint('customer_id', name=f'unique_customer_constraint_{table_name}')
        )
        metadata.create_all(engine)
    else:
        print(f"Table {table_name} already exists, skipping creation.")

def extract_data(**kwargs):
    """
    Extract data from source database using SQL query and push to XCom.
    """
    ti = kwargs['ti']
    hook = PostgresHook('source_db')
    conn = hook.get_conn()
    sql = """
        SELECT
            c.customer_id, c.begin_date, c.end_date, c.type, c.paperless_billing, 
            c.payment_method, c.monthly_charges, c.total_charges,
            i.internet_service, i.online_security, i.online_backup, 
            i.device_protection, i.tech_support, i.streaming_tv, i.streaming_movies,
            p.gender, p.senior_citizen, p.partner, p.dependents,
            ph.multiple_lines
        FROM contracts AS c
        LEFT JOIN internet AS i ON i.customer_id = c.customer_id
        LEFT JOIN personal AS p ON p.customer_id = c.customer_id
        LEFT JOIN phone AS ph ON ph.customer_id = c.customer_id
    """
    data = pd.read_sql(sql, conn)
    conn.close()
    ti.xcom_push(key='extracted_data', value=data)

def transform_data(**kwargs):
    """
    Transform extracted data by adding target column and cleaning end_date.
    """
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data', key='extracted_data')
    data['target'] = (data['end_date'] != 'No').astype(int)
    data['end_date'] = data['end_date'].replace({'No': None})
    ti.xcom_push(key='transformed_data', value=data)

def load_data(**kwargs):
    """
    Load transformed data into the destination database.
    """
    ti = kwargs['ti']
    table_name = kwargs.get('table_name', 'alt_users_churn')
    data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table=table_name,
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['customer_id'],
        rows=data.values.tolist()
    )