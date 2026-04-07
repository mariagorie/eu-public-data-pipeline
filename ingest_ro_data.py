import pandas as pd
import eurostat
from prefect import flow, task
import snowflake.connector

@task(log_prints=True)
def get_ro_stats_live():
    print("🚀 Extragere date LIVE de la Eurostat...")
    
    # Luăm tabelul de populație 'demo_pjan'
    # geo='RO' (România), unit='NR' (Număr persoane), age='TOTAL', sex='T' (Total)
    df_raw = eurostat.get_data_df('demo_pjan', filter_pars={'geo': 'RO', 'unit': 'NR', 'age': 'TOTAL', 'sex': 'T'})
    
    # Eurostat pune anii ca nume de coloane. Căutăm coloanele care arată a ani (ex: 2023, 2022)
    # Folosim list comprehension pentru a găsi coloanele numerice
    year_cols = [col for col in df_raw.columns if str(col).strip().isdigit()]
    
    # Sortăm anii și îi luăm pe ultimii 5
    recent_years = sorted(year_cols, key=lambda x: int(str(x).strip()))[-5:]
    
    data_for_pandas = []
    for yr in recent_years:
        # Luăm valoarea din prima linie (RO) pentru anul respectiv
        val = df_raw[yr].iloc[0]
        data_for_pandas.append({
            'YEAR': int(str(yr).strip()), 
            'POPULATION_COUNT': float(val)
        })
    
    # Aici creăm df-ul final
    df_final = pd.DataFrame(data_for_pandas)
    print(f"✅ Date procesate pentru anii: {df_final['YEAR'].tolist()}")
    return df_final

@task(log_prints=True)
def load_to_snowflake(df_to_load):
    print("❄️ Conectare la Snowflake...")
    conn = snowflake.connector.connect(
        user='MARIAGORIE',
        password='SxN5UAhgSKjrYF6', # <--- Pune parola ta!
        account='PPBAWEF-SQ30327',
        warehouse='COMPUTE_WH',
        database='ROMANIA_STATS_DB',
        schema='RAW'
    )
    
    cursor = conn.cursor()
    
    # Ne asigurăm că tabelul există cu structura corectă
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS RAW.EUROSTAT_POPULATION (
            YEAR INT, 
            POPULATION_COUNT FLOAT,
            INSERTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    
    cursor.execute("TRUNCATE TABLE RAW.EUROSTAT_POPULATION")
    
    # Inserăm datele din DataFrame-ul primit
    for _, row in df_to_load.iterrows():
        cursor.execute(
            "INSERT INTO RAW.EUROSTAT_POPULATION (YEAR, POPULATION_COUNT) VALUES (%s, %s)",
            (int(row['YEAR']), float(row['POPULATION_COUNT']))
        )
    
    conn.close()
    print(f"✨ Succes! {len(df_to_load)} rânduri au ajuns în Snowflake.")

@flow(name="Eurostat to Snowflake Pipeline")
def eurostat_flow():
    # Pasul 1: Extracție (rezultatul e salvat în variabila 'my_df')
    my_df = get_ro_stats_live()
    
    # Pasul 2: Încărcare (pasăm 'my_df' către funcția de load)
    load_to_snowflake(my_df)

if __name__ == "__main__":
    eurostat_flow()