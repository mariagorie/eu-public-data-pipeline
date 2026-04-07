import pandas as pd
import eurostat
from prefect import flow, task
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

@task(log_prints=True)
def fetch_gdp_data():
    print("Fetching GDP data per NUTS 3 regions for Romania...")
    # 'nama_10r_3gdp' e codul pentru PIB regional
    df = eurostat.get_data_df('nama_10r_3gdp', filter_pars={'geo': 'RO', 'unit': 'MIO_EUR'})
    
    # Transpunem anii din coloane în rânduri
    year_cols = [col for col in df.columns if str(col).strip().isdigit()]
    df_melted = df.melt(id_vars=['unit', 'geo'], value_vars=year_cols, var_name='YEAR', value_name='GDP_VALUE')
    
    # Curățăm YEAR să fie INT
    df_melted['YEAR'] = df_melted['YEAR'].apply(lambda x: int(str(x).strip()))
    return df_melted

@task(log_prints=True)
def upload_to_snowflake(df):
    conn = snowflake.connector.connect(
        user='MARIAGORIE',
        password='SxN5UAhgSKjrYF6',
        account='PPBAWEF-SQ30327',
        warehouse='COMPUTE_WH',
        database='ROMANIA_STATS_DB',
        schema='RAW'
    )
    # Metoda de "Senior": write_pandas creează tabelul automat și e mult mai rapidă
    write_pandas(conn, df, table_name='RAW_GDP_ROMANIA', auto_create_table=True, overwrite=True)
    conn.close()
    print("Done! Data is in Snowflake.")

@flow(name="Economy Ingestion Pipeline")
def economy_flow():
    data = fetch_gdp_data()
    upload_to_snowflake(data)

if __name__ == "__main__":
    economy_flow()