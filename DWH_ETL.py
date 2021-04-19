from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'adubinsky'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_dwh_etl',
    default_args = default_args,
    description = 'Data Lake ETL tasks',
    schedule_interval = "0 0 1 * *",
)

ods_payment_trunc = PostgresOperator(
    task_id="ods_payment",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
      alter table adubinsky.ods_payment truncate PARTITION {1}"{{execution_date.strftime("%Y%m")}}}";
    """.format('p')
    )

ods_payment = PostgresOperator(
    task_id="ods_payment",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
                insert into adubinsky.ods_payment
        (
          user_id , 
          pay_doc_type , 
          pay_doc_num  , 
          account , 
          phone , 
          billing_period , 
          pay_date  , 
          sum ,
          src_name
        )
        select  user_id , 
          pay_doc_type , 
          pay_doc_num  , 
          account , 
          phone , 
          billing_period , 
          pay_date  , 
          sum ,
          src_name
          from adubinsky.v_stg_ods_payment
        where pay_date between "{{ execution_date.strftime("%Y-%m-%d")}}"::TIMESTAMP  and "{{ execution_date.strftime("%Y-%m-%d")}}":TIMESTAMP  + interval '1 month' - interval '1 second';
    """
)
