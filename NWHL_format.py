import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
import os

user=os.getenv('MYSQL_user')
pw=os.getenv('MYSQL')
str_sql = 'mysql+mysqlconnector://' + user + ':' + pw + '@localhost'
m_engine = create_engine(str_sql)

root_in = '~/Documents/PythonProjects/Airflow_Project/airflow-pipeline-amanda-wink/input_files/'
root_out = '~/Documents/PythonProjects/Airflow_Project/airflow-pipeline-amanda-wink/output_files/'
def NWHL_2017_format(root_in=root_in, root_out=root_out):
    NWHL_2017 = pd.read_excel(root_in + 'NWHL Skater and Team Stats 2017-18.xlsx', sheet_name='NWHL Skaters 201718')
    NWHL_2017.drop(columns=['SOG', 'Sh%', 'SOG/G'], inplace=True)
    NWHL_2017.rename(columns={'Tm': 'Team', 'Name': 'Player', 'P': 'Pos', 'P.1':'P', 'EN': 'ENG', 'Pts/G': 'Pts/GP'}, inplace=True)
    NWHL_2017.insert(1, 'League', ['NWHL'] * len(NWHL_2017))
    NWHL_2017['Rk'].fillna('N', inplace=True)
    keys = NWHL_2017.columns[15:33]
    fill_dict = {key: 0 for key in keys}
    NWHL_2017.fillna(fill_dict, inplace=True)
    NWHL_2017['GP'].fillna('0', inplace=True)
    NWHL_2017['Pl/Mi'] = [None] * len(NWHL_2017)
    NWHL_2017.to_csv(root_out + '/NWHL_2017.csv', index=False)

def CWHL_2017_format(root_in=root_in, root_out=root_out):
    CWHL_2017 = pd.read_excel(root_in + 'CWHL Skater and Team Stats 2017-18.xlsx', sheet_name='Skaters')
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
    CWHL_2017_format.to_csv(root_out + 'CWHL_2017.csv', index=False)

def Combine(root_out=root_out):
    NWHL_2017 = pd.read_csv(root_out + 'NWHL_2017.csv')
    CWHL_2017_format = pd.read_csv(root_out + 'CWHL_2017.csv')
    WIH_2017 = pd.concat([NWHL_2017, CWHL_2017_format], ignore_index=True)
    WIH_2017.to_csv(root_out + 'W_IceHockey_2017.csv', index=False)
    
def convert_to_sql(root_out=root_out, engine=m_engine):
    ih_df = pd.read_csv(root_out + 'W_IceHockey_2017.csv')
    if engine.has_table('w_h'):
        engine.execute('DROP TABLE w_h;')
    ih_df.to_sql(name='w_h', con=engine)
    engine.dispose()

def print_stats(engine=m_engine):
    with engine.connect() as connection:
        result_CWHL = connection.execute(
            'select G, Player, No, League, Team from w_h where League = "CWHL" order by G DESC LIMIT 5;')
        result_NWHL = connection.execute(
            'select G, Player, No, League, Team from w_h where League = "NWHL" order by G DESC LIMIT 5;')
        result_all = connection.execute('select G, Player, No, League, Team from w_h order by G DESC LIMIT 20;')
        print_CWHL_results = result_CWHL.fetchall()
        print_NWHL_results = result_NWHL.fetchall()
        print_all_results = result_all.fetchall()
        print("Top CWHL Goal Scorers 2017 \n")
        for row in range(len(print_CWHL_results)):
            for val in print_CWHL_results[row]:
                print(str(val), end=" ")
            print(" \n")
        print("Top NWHL Goal Scorers 2017")
        for row in range(len(print_NWHL_results)):
            for val in print_NWHL_results[row]:
                print(str(val), end=" ")
            print(" \n")
        print("Top Goal Scorers 2017")
        for row in range(len(print_all_results)):
            for val in print_all_results[row]:
                print(str(val), end=" ")
            print(" \n")

dag = DAG('NWHL_2017_table_format', description='Formats NWHL 2017 data',
          schedule_interval='@once',
          start_date=datetime(2020, 11, 29), catchup=False)

NWHL_2017_operator = PythonOperator(task_id='NWHL_2017_format', python_callable=NWHL_2017_format, dag=dag)
CWHL_2017_operator = PythonOperator(task_id='CWHL_2017_format', python_callable=CWHL_2017_format, dag=dag)
Combine_operator = PythonOperator(task_id='Combine_tables', python_callable=Combine, dag=dag)
Print_stat_operator = PythonOperator(task_id='Print_stat', python_callable=print_stats, dag=dag)


[NWHL_2017_operator, CWHL_2017_operator] >> Combine_operator >> SQL_operator >> Print_stat_operator