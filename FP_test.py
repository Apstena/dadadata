
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
        where greatest(start_time,coalesce(end_time,'1900-01-01'::timestamp)) between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 year' - interval '1 second'
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
          where not exists (select 1 from adubinsky.fp_ods_mdm_user m where v.user_id=m.user_id and m.exp_dttm='2999-12-31 00:00:00'::timestamp)
          ;
    """
)

all_ods_loaded = DummyOperator(task_id="all_ods_loaded", dag=dag)

ods_payment >> all_ods_loaded
ods_payment >> all_ods_loaded
ods_billing >> all_ods_loaded
ods_traffic >> all_ods_loaded
ods_issue >> all_ods_loaded
ods_mdm_user >> all_ods_loaded

dds_hub_segment = PostgresOperator(
    task_id="dds_hub_segment",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql=""""
		insert into adubinsky.fp_dds_hub_segment
		(
		segment_key,
		segment_nkey,
		load_dttm,
		src_name,
		eff_dt
		)
		select distinct
		segment_key,
		segment_nkey,
		load_dttm,
		src_name,
		'{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as eff_dt
		from adubinsky.fp_v_ods_mdm_user v
		where exp_dttm='2999-12-31 00:00:00'
		and not exists (
		  select 1 from adubinsky.fp_dds_hub_segment h where v.segment_key=h.segment_key
		)
		;
    """
)
dds_hub_distr = PostgresOperator(
    task_id="dds_hub_distr",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql=""""
		insert into adubinsky.fp_dds_hub_distr
		(
		distr_key,
		distr_nkey,
		load_dttm,
		src_name,
		eff_dt
		)
		select distinct
		distr_key,
		distr_nkey,
		load_dttm,
		src_name,
		'{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as eff_dt
		from adubinsky.fp_v_ods_mdm_user v
		where exp_dttm='2999-12-31 00:00:00'
		and not exists (
		  select 1 from adubinsky.fp_dds_hub_distr h where v.distr_key=h.distr_key
		)
		;
    """
)
dds_hub_reg_per = PostgresOperator(
    task_id="dds_hub_reg_per",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql=""""
		insert into adubinsky.fp_dds_hub_reg_per
		(
		reg_per_key,
		reg_per_nkey,
		load_dttm,
		src_name,
		eff_dt
		)
		select distinct
		reg_per_key,
		reg_per_nkey,
		load_dttm,
		src_name,
		'{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as eff_dt
		from adubinsky.fp_v_ods_mdm_user v
		where exp_dttm='2999-12-31 00:00:00'
		and not exists (
		  select 1 from adubinsky.fp_dds_hub_reg_per h where v.reg_per_key=h.reg_per_key
		)
		;
    """
)
dds_hub_billing_mode = PostgresOperator(
    task_id="dds_hub_billing_mode",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql=""""
		insert into adubinsky.fp_dds_hub_billing_mode
		(
		billing_mode_key,
		billing_mode_nkey,
		load_dttm,
		src_name,
		eff_dt
		)
		select distinct
		billing_mode_key,
		billing_mode_nkey,
		load_dttm,
		src_name,
		'{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as eff_dt
		from adubinsky.fp_v_ods_mdm_user v
		where exp_dttm='2999-12-31 00:00:00'
		and not exists (
		  select 1 from adubinsky.fp_dds_hub_billing_mode h where v.billing_mode_key=h.billing_mode_key
		)
		;
    """
)
dds_hub_users = PostgresOperator(
    task_id="dds_hub_users",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql=""""
		insert into adubinsky.fp_dds_hub_users
		(
		user_key,
		user_nkey,
		load_dttm,
		src_name,
		eff_dt
		)
		select distinct
		user_key,
		user_nkey,
		load_dttm,
		src_name,
		'{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as eff_dt
		from adubinsky.fp_v_ods_mdm_user v
		where exp_dttm='2999-12-31 00:00:00'
		and not exists (
		  select 1 from adubinsky.fp_dds_hub_users h where v.user_key=h.user_key
		)
		;
    """
)
dds_hub_payment = PostgresOperator(
    task_id="dds_hub_payment",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql=""""
		insert into adubinsky.fp_dds_hub_payment
		(
		payment_key,
		payment_nkey,
		load_dttm,
		src_name,
		eff_dt
		)
		select distinct
		payment_key,
		payment_nkey,
		load_dttm,
		src_name,
		'{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as eff_dt
		from adubinsky.fp_v_ods_payment v
		where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP
		and not exists (
		  select 1 from adubinsky.fp_dds_hub_payment h where v.payment_key=h.payment_key
		)
		;
    """
)
dds_hub_issue = PostgresOperator(
    task_id="dds_hub_issue",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql=""""
		insert into adubinsky.fp_dds_hub_issue
		(
		issue_key,
		issue_nkey,
		load_dttm,
		src_name,
		eff_dt
		)
		select distinct
		issue_key,
		issue_nkey,
		load_dttm,
		src_name,
		'{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as eff_dt
		from adubinsky.fp_v_ods_issue v
		where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP
		and not exists (
		  select 1 from adubinsky.fp_dds_hub_issue h where v.issue_key=h.issue_key
		)
		;
    """
)
dds_hub_billing = PostgresOperator(
    task_id="dds_hub_billing",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql=""""
		insert into adubinsky.fp_dds_hub_billing
		(
		billing_key,
		billing_nkey,
		load_dttm,
		src_name,
		eff_dt
		)
		select distinct
		billing_key,
		billing_nkey,
		load_dttm,
		src_name,
		'{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as eff_dt
		from adubinsky.fp_v_ods_billing v
		where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP
		and not exists (
		  select 1 from adubinsky.fp_dds_hub_billing h where v.billing_key=h.billing_key
		)
		;
    """
)
dds_hub_traffic = PostgresOperator(
    task_id="dds_hub_traffic",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql=""""
		insert into adubinsky.fp_dds_hub_traffic
		(
		traffic_key,
		traffic_nkey,
		load_dttm,
		src_name,
		eff_dt
		)
		select distinct
		traffic_key,
		traffic_nkey,
		load_dttm,
		src_name,
		'{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as eff_dt
		from adubinsky.fp_v_ods_traffic v
		where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP
		and not exists (
		  select 1 from adubinsky.fp_dds_hub_traffic h where v.traffic_key=h.traffic_key
		)
		;
    """
)
dds_hub_device = PostgresOperator(
    task_id="dds_hub_device",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql=""""
		insert into adubinsky.fp_dds_hub_device
		(
		device_key,
		device_nkey,
		load_dttm,
		src_name,
		eff_dt
		)
		select distinct
		device_key,
		device_nkey,
		load_dttm,
		src_name,
		'{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as eff_dt
		from adubinsky.fp_v_ods_traffic v
		where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP
		and not exists (
		  select 1 from adubinsky.fp_dds_hub_device h where v.device_key=h.device_key
		)
		;
    """
)
dds_hub_tariff = PostgresOperator(
    task_id="dds_hub_tariff",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql=""""
		insert into adubinsky.fp_dds_hub_tariff
		(
		tariff_key,
		tariff_nkey,
		load_dttm,
		src_name,
		eff_dt
		)
		select distinct
		tariff_key,
		tariff_nkey,
		load_dttm,
		src_name,
		'{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as eff_dt
		from adubinsky.fp_v_ods_billing v
		where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP
		and not exists (
		  select 1 from adubinsky.fp_dds_hub_tariff h where v.tariff_key=h.tariff_key
		)
		;
    """
)
dds_hub_service = PostgresOperator(
    task_id="dds_hub_service",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql=""""
		insert into adubinsky.fp_dds_hub_service
		(
		service_key,
		service_nkey,
		load_dttm,
		src_name,
		eff_dt
		)
		select distinct
		service_key,
		service_nkey,
		load_dttm,
		src_name,
		'{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as eff_dt
		from adubinsky.fp_v_ods_billing v
		where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP
		and not exists (
		  select 1 from adubinsky.fp_dds_hub_service h where v.service_key=h.service_key
		)
		;
    """
)
dds_hub_billing_per = PostgresOperator(
    task_id="dds_hub_billing_per",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql=""""
		insert into adubinsky.fp_dds_hub_billing_per
		(
		billing_per_key,
		billing_per_nkey,
		load_dttm,
		src_name,
		eff_dt
		)
		select distinct
		billing_per_key,
		billing_per_nkey,
		load_dttm,
		src_name,
		'{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as eff_dt
		from adubinsky.fp_v_ods_billing v
		where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP
		and not exists (
		  select 1 from adubinsky.fp_dds_hub_billing_per h where v.billing_per_key=h.billing_per_key
		)
		;
    """
)
dds_hub_payment_doc_type = PostgresOperator(
    task_id="dds_hub_payment_doc_type",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql=""""
		insert into adubinsky.fp_dds_hub_payment_doc_type
		(
		payment_doc_type_key,
		payment_doc_type_nkey,
		load_dttm,
		src_name,
		eff_dt
		)
		select distinct
		payment_doc_type_key,
		payment_doc_type_nkey,
		load_dttm,
		src_name,
		'{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP as eff_dt
		from adubinsky.fp_v_ods_payment v
		where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP
		and not exists (
		  select 1 from adubinsky.fp_dds_hub_payment_doc_type h where v.payment_doc_type_key=h.payment_doc_type_key
		)
		;
    """
)
