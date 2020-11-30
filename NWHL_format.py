import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def NWHL_2017_format():
    NWHL_2017 = pd.read_excel('~/Documents/PythonProjects/Airflow_Project/NWHL Skater and Team Stats 2017-18.xlsx', sheet_name='NWHL Skaters 201718')
    NWHL_2017.drop(columns=['SOG', 'Sh%', 'SOG/G'], inplace=True)
    NWHL_2017.rename(columns={'Tm': 'Team', 'Name': 'Player', 'P': 'Pos', 'P.1':'P', 'EN': 'ENG', 'Pts/G': 'Pts/GP'}, inplace=True)
    NWHL_2017.insert(1, 'League', ['NWHL'] * len(NWHL_2017))
    NWHL_2017['Rk'].fillna('N', inplace=True)
    keys = NWHL_2017.columns[15:33]
    fill_dict = {key: 0 for key in keys}
    NWHL_2017.fillna(fill_dict, inplace=True)
    NWHL_2017['GP'].fillna('0', inplace=True)
    NWHL_2017['Pl/Mi'] = [None] * len(NWHL_2017)
    NWHL_2017.to_csv('~/Documents/PythonProjects/Airflow_Project/NWHL_2017.csv', index=False)

def CWHL_2017_format():
    CWHL_2017 = pd.read_excel('~/Documents/PythonProjects/Airflow_Project/CWHL Skater and Team Stats 2017-18.xlsx', sheet_name='Skaters')
    CWHL_2017.rename(columns={'Name': 'Player', '#': 'No'}, inplace=True)
    CWHL_2017.drop(labels=[152, 153, 154, 155], inplace=True)
    keys = CWHL_2017.columns[10:29]
    fill_dict = {key: 0 for key in keys}
    CWHL_2017.fillna(fill_dict, inplace=True)
    CWHL_2017_format = CWHL_2017[['No', 'Team', 'Player', 'Pos']]
    end_col = list(CWHL_2017.columns[4:])
    CWHL_2017_df2 = CWHL_2017[end_col]
    CWHL_2017_format = pd.concat([CWHL_2017_format, CWHL_2017_df2], axis=1)
    empty_col = [None] * len(CWHL_2017_format)
    CWHL_2017_format.insert(4, 'Ht', empty_col)
    CWHL_2017_format.insert(5, 'Rk', empty_col)
    CWHL_2017_format.insert(6, 'S', empty_col)
    CWHL_2017_format.insert(7, 'Age', empty_col)
    CWHL_2017_format.insert(8, 'Nat', empty_col)
    CWHL_2017_format.insert(1, 'League', ['CWHL'] * len(CWHL_2017_format))
    CWHL_2017_format.to_csv('~/Documents/PythonProjects/Airflow_Project/CWHL_2017.csv', index=False)

def Combine():
    NWHL_2017 = pd.read_csv('~/Documents/PythonProjects/Airflow_Project/NWHL_2017.csv')
    CWHL_2017_format = pd.read_csv('~/Documents/PythonProjects/Airflow_Project/CWHL_2017.csv')
    WIH_2017 = pd.concat([NWHL_2017, CWHL_2017_format], ignore_index=True)
    WIH_2017.to_csv('~/Documents/PythonProjects/Airflow_Project/W_IceHockey_2017.csv', index=False)

dag = DAG('NWHL_2017_table_format', description='Formats NWHL 2017 data',
          schedule_interval='@once',
          start_date=datetime(2020, 11, 29), catchup=False)

NWHL_2017_operator = PythonOperator(task_id='NWHL_2017_format', python_callable=NWHL_2017_format, dag=dag)
CWHL_2017_operator = PythonOperator(task_id='CWHL_2017_format', python_callable=CWHL_2017_format, dag=dag)
Combine_operator = PythonOperator(task_id='Combine_tables', python_callable=Combine, dag=dag)


[NWHL_2017_operator, CWHL_2017_operator] >> Combine_operator