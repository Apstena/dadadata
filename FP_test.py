
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
          tech_dt,
          del_ind
        )
        select 
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
          tech_dt,
          case when rn = 1 then 0
            else 1 end as del_ind
          from  
        (select  user_id , 
          pay_doc_type , 
          pay_doc_num  , 
          account , 
          phone , 
          billing_period , 
          pay_date  , 
          sum ,
          src_name,
          load_dttm,
          '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as tech_dt,
          row_number() over (partition by pay_doc_num order by pay_date desc) as rn
          from adubinsky.fp_v_stg_ods_payment
        where pay_date between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 year' - interval '1 second'
            ) pre
        ;
    """
)


ods_billing = PostgresOperator(
    task_id="ods_billing",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
         alter table adubinsky.fp_ods_billing truncate PARTITION "p{{execution_date.strftime('%Y')}}";
         Insert into adubinsky.fp_ods_billing
        (
          user_id , 
          billing_period , 
          service  , 
          tariff , 
          sum , 
          created_at , 
          src_name,
          load_dttm,
          tech_dt,
          del_ind
        )
        select
        user_id , 
          billing_period , 
          service  , 
          tariff , 
          sum , 
          created_at , 
          src_name,
          load_dttm,
          tech_dt,
           case when rn = 1 then 0
            else 1 end as del_ind
        from (
        select  
          user_id , 
          billing_period , 
          service  , 
          tariff , 
          sum , 
          created_at , 
          src_name,
          load_dttm,
          '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as tech_dt,
          row_number() over (partition by user_id,billing_period,service,tariff order by created_at desc) as rn
          from adubinsky.fp_v_stg_ods_billing
        where created_at between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 year' - interval '1 second'
        )pre;
    """
)


ods_traffic = PostgresOperator(
    task_id="ods_traffic",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
         alter table adubinsky.fp_ods_traffic truncate PARTITION "p{{execution_date.strftime('%Y')}}";
         Insert into adubinsky.fp_ods_traffic
        (
          user_id , 
          timestamp_src , 
          device_id  , 
          device_ip_addr , 
          bytes_sent , 
          bytes_received , 
          src_name,
          load_dttm,
          tech_dt,
          del_ind
        )
        select  
          user_id , 
          timestamp , 
          device_id  , 
          device_ip_addr , 
          bytes_sent , 
          bytes_received , 
          src_name,
          load_dttm,
          tech_dt,
          case when rn =1 then 0 
          else 1 end as del_ind      
        from (
        select  
          user_id , 
          timestamp , 
          device_id  , 
          device_ip_addr , 
          bytes_sent , 
          bytes_received , 
          src_name,
          load_dttm,
          '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as tech_dt,
          row_number() over(partition by user_id,timestamp, device_id order by  bytes_sent+bytes_received desc) as rn
          from adubinsky.fp_v_stg_ods_traffic
        where timestamp between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 year' - interval '1 second'
        ) pre
        ;
    """
)


ods_issue = PostgresOperator(
    task_id="ods_issue",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
         alter table adubinsky.fp_ods_issue truncate PARTITION "p{{execution_date.strftime('%Y')}}";
         update adubinsky.fp_ods_issue o
         set del_ind=0
         from adubinsky.fp_v_stg_ods_issue n
         where n.hashsum=o.hashsum \
         and o.tech_dt<'{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP
         and greatest(n.start_time,coalesce(n.end_time,'1900-01-01'::timestamp)) between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  
            and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 year' - interval '1 second';
         Insert into adubinsky.fp_ods_issue
        (
          user_id , 
          start_time , 
          end_time  , 
          title , 
          description , 
          service , 
          src_name,
          load_dttm,
          tech_dt,
          del_ind,
          hashsum
        )
          select
          user_id , 
          start_time , 
          end_time  , 
          title , 
          description , 
          service ,
          src_name,
          load_dttm,
          tech_dt,
          case when rn=1 then 0
          else 1 end as del_ind, 
          hashsum
          from
        (select  
          user_id , 
          start_time , 
          end_time  , 
          title , 
          description , 
          service ,
          src_name,
          load_dttm,
          '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as tech_dt,
          hashsum,
          row_number() over (partition by hashsum order by end_time desc) rn
          from adubinsky.fp_v_stg_ods_issue
        where greatest(start_time,coalesce(end_time,'1900-01-01'::timestamp) between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 year' - interval '1 second'
        )pre
        ;
    """
)


ods_mdm_user = PostgresOperator(
    task_id="ods_mdm_user",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
          insert into  adubinsky.fp_ods_mdm_user
         (
         user_id , 
          legal_type , 
          district  , 
          registered_at , 
          billing_mode , 
          is_vip , 
          src_name ,
          load_dttm ,
          hashsum ,
          eff_dttm ,
          exp_dttm ,
          del_ind
         )
         select  
         user_id,
         legal_type , 
          district  , 
          registered_at , 
          billing_mode , 
          is_vip , 
          src_name ,
          load_dttm ,
          hashsum ,
          '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as eff_dttm ,
          '2999-12-31 00:00:00'::timestamp as exp_dttm ,
          1 as del_ind
          from adubinsky.fp_ods_mdm_user m
         where not exists (select 1 from adubinsky.fp_v_mdm_ods_mdm_user v where v.user_id=m.user_id)
         and m.exp_dttm='2999-12-31 00:00:00'::timestamp ;
         
         update adubinsky.fp_ods_mdm_user m
         set exp_dttm = '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP - interval '1 second'
         where not exists (select 1 from adubinsky.fp_v_mdm_ods_mdm_user v where v.user_id=m.user_id)
         and m.exp_dttm='2999-12-31 00:00:00'::timestamp and del_ind = 0;
         
          insert into  adubinsky.fp_ods_mdm_user
         (
         user_id , 
          legal_type , 
          district  , 
          registered_at , 
          billing_mode , 
          is_vip , 
          src_name ,
          load_dttm ,
          hashsum ,
          eff_dttm ,
          exp_dttm ,
          del_ind
         )
         select  
         user_id,
         legal_type , 
          district  , 
          registered_at , 
          billing_mode , 
          is_vip , 
          src_name ,
          load_dttm ,
          hashsum ,
          '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as eff_dttm ,
          '2999-12-31 00:00:00'::timestamp as exp_dttm ,
          0 as del_ind
          from adubinsky.fp_v_mdm_ods_mdm_user v
          where exists (select 1 from adubinsky.fp_ods_mdm_user m where v.user_id=m.user_id and (m.hashsum!=v.hashsum or m.del_ind=1)
                        and m.exp_dttm='2999-12-31 00:00:00'::timestamp)
          ;

          update adubinsky.fp_ods_mdm_user m
         set exp_dttm = '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP - interval '1 second'
         where exists (select 1 from adubinsky.fp_v_mdm_ods_mdm_user v where v.user_id=m.user_id and (m.hashsum!=v.hashsum or m.del_ind=1))
         and m.exp_dttm='2999-12-31 00:00:00'::timestamp;
    """
)
