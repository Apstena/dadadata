
from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'adubinsky'
default_args = {
    'owner': USERNAME,
    'depends_on_past' : True,
     'wait_for_downstream' : True,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_fp_test',
    default_args = default_args,
    description = 'DWH ETL tasks',
    schedule_interval = "0 0 1 1 *",
)
#Очистка и заполнение партиций ODS фактов очищенной на прошлом шаге партиции
ods_payment = PostgresOperator(
    task_id="ods_payment",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
         alter table adubinsky.fp_ods_payment truncate PARTITION "p{{execution_date.strftime('%Y')}}";
         Insert into adubinsky.fp_ods_payment
        (
          user_id , 
          pay_doc_type , 
          pay_doc_num  , 
          account , 
          phone , 
          billing_period , 
          pay_date  , 
          sum ,
          src_name,
          load_dttm,
          tech_dt
        )
        select  user_id , 
          pay_doc_type , 
          pay_doc_num  , 
          account , 
          phone , 
          billing_period , 
          pay_date  , 
          sum ,
          src_name,
          load_dttm,
          '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as tech_dt
          
          from adubinsky.fp_v_stg_ods_payment
        where pay_date between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 year' - interval '1 second';
    """
)
