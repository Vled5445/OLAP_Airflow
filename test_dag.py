from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from clickhouse_driver import Client
from sqlalchemy import create_engine
default_args = {'owner':'vlados','start_date': datetime(2024, 7, 22)}


dag = DAG(dag_id='test_dag',
          default_args=default_args, schedule_interval='5-55/15 * * * *',
          description= 'super_dag' ,
          catchup=False, max_active_runs=1
          )


def main():
    client = Client('some-clickhouse-server',
                    user='airflow',
                    password='airflow',
                    port=9000,
                    verify=False,
                    database= 'default',
                     settings={"numpy_columns": False,'usenumpy': True},
                     compression=False)
    client.execute("""insert into report.gi_agg as select toDate(dt) dt_hour ,
    employee_id ,
    office_id ,
    wh_id ,
    uniq(gi) ,
    status_id
    from gi 
    group by dt_hour,employee_id,office_id,wh_id,status_id""")
    df = client.query_dataframe('select * from report.gi_agg')
    engine = create_engine('postgresql://default:5445@some-postgres:5432/report')
    df.to_sql('report.gi_agg',engine,if_exists='append')
task1 = PythonOperator(
    task_id='report_gi', python_callable=main, dag=dag
)
