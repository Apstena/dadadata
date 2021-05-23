
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

#Блок загрузки хабов DDS

dds_hub_segment = PostgresOperator(
    task_id="dds_hub_segment",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
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
    sql="""
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
    sql="""
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
    sql="""
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
    sql="""
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
    sql="""
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
    sql="""
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
    sql="""
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
    sql="""
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
    sql="""
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
    sql="""
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
    sql="""
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
    sql="""
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
    sql="""
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


all_ods_loaded >>  dds_hub_segment
all_ods_loaded >>  dds_hub_distr
all_ods_loaded >>  dds_hub_reg_per
all_ods_loaded >>  dds_hub_billing_mode
all_ods_loaded >>  dds_hub_users
all_ods_loaded >>  dds_hub_payment
all_ods_loaded >>  dds_hub_issue
all_ods_loaded >>  dds_hub_billing
all_ods_loaded >>  dds_hub_traffic
all_ods_loaded >>  dds_hub_device
all_ods_loaded >>  dds_hub_tariff
all_ods_loaded >>  dds_hub_service
all_ods_loaded >>  dds_hub_billing_per
all_ods_loaded >>  dds_hub_payment_doc_type

#Блок загрузки сателлитов DDS

dds_sat_reg_per = PostgresOperator(
    task_id="dds_sat_reg_per",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_sat_reg_per s
set exp_dt = v.eff_dttm - interval '1 second'
from adubinsky.fp_v_ods_mdm_user v
where '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp between v.eff_dttm and v.exp_dttm
and v.reg_per_hashsum!=s.hashsum and v.reg_per_key=s.reg_per_key;

update adubinsky.fp_dds_sat_reg_per s
set exp_dt = '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp - interval '1 second'
where 
not exists(select 1 from adubinsky.fp_v_ods_mdm_user v where
'{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp between v.eff_dttm and v.exp_dttm
and v.reg_per_key=s.reg_per_key);

insert into adubinsky.fp_dds_sat_reg_per
(
reg_per_key,
reg_year,
reg_year_month,
load_dttm,
eff_dt,
exp_dt,
hashsum
)
select distinct
v.reg_per_key,
(v.reg_per/100)::int as reg_year,
v.reg_per::int as reg_year_month,
v.load_dttm,
v.eff_dttm,
v.exp_dttm,
v.reg_per_hashsum
from adubinsky.fp_v_ods_mdm_user v
where '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp between v.eff_dttm and v.exp_dttm
and not exists (select 1 from adubinsky.fp_dds_sat_reg_per s where v.reg_per_key=s.reg_per_key and s.exp_dt='2999-12-31 00:00:00');
    """
)

dds_sat_user_prop = PostgresOperator(
    task_id="dds_sat_user_prop",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_sat_user_prop s
set exp_dt = v.eff_dttm - interval '1 second'
from 
(select v.user_key,min(v.eff_dt) as eff_dttm from
(
select 
user_key,
account,
phone,
load_dttm,
eff_dt,
exp_dt,
hashsum,
case when eff_dt = min(eff_dt) over(partition by user_key) then 1 else 0 end as frow
from(
select 
user_key,
account,
phone,
load_dttm,
eff_dt,
lead(eff_dt,1,'2999-12-31') over (partition by user_key order by scng) -interval '1 second' as exp_dt,
user_prop_hashsum as hashsum 
from(
select distinct
user_key,
account,
phone,
user_prop_hashsum,
load_dttm,
min(eff_dt) over (partition by user_key,scng) as eff_dt,
scng
from(
select 
user_key,
account,
phone,
user_prop_hashsum,
load_dttm,
eff_dt,
sum(cng) over (partition by user_key order by rn) as scng
from(
select 
user_key,
account,
phone,
user_prop_hashsum,
load_dttm,
pay_date as eff_dt,
row_number() over (partition by user_key order by pay_date) as rn,
case when 
  lag(user_prop_hashsum,1,user_prop_hashsum) over (partition by user_key order by pay_date)!=user_prop_hashsum
  then 1 else 0 end as cng
from adubinsky.fp_v_ods_payment 
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
)sq1
)sq2
)sq3
)sq4
where eff_dt<exp_dt  
) v
join adubinsky.fp_dds_sat_user_prop sv on v.user_key=sv.user_key and sv.exp_dt='2999-12-31' and v.hashsum!=sv.hashsum
group by v.user_key)v
where s.user_key=v.user_key and s.exp_dt='2999-12-31'
;
insert into adubinsky.fp_dds_sat_user_prop
(
user_key,
user_account,
user_phone,
load_dttm,
eff_dt,
exp_dt,
hashsum
)
select
sq5.user_key,
sq5.account,
sq5.phone,
sq5.load_dttm,
sq5.eff_dt,
sq5.exp_dt,
sq5.hashsum
from(
select 
user_key,
account,
phone,
load_dttm,
eff_dt,
exp_dt,
hashsum,
case when eff_dt = min(eff_dt) over(partition by user_key) then 1 else 0 end as frow
from(
select 
user_key,
account,
phone,
load_dttm,
eff_dt,
lead(eff_dt,1,'2999-12-31') over (partition by user_key order by scng) -interval '1 second' as exp_dt,
user_prop_hashsum as hashsum 
from(
select distinct
user_key,
account,
phone,
user_prop_hashsum,
load_dttm,
min(eff_dt) over (partition by user_key,scng) as eff_dt,
scng
from(
select 
user_key,
account,
phone,
user_prop_hashsum,
load_dttm,
eff_dt,
sum(cng) over (partition by user_key order by rn) as scng
from(
select 
user_key,
account,
phone,
user_prop_hashsum,
load_dttm,
pay_date as eff_dt,
row_number() over (partition by user_key order by pay_date) as rn,
case when 
  lag(user_prop_hashsum,1,user_prop_hashsum) over (partition by user_key order by pay_date)!=user_prop_hashsum
  then 1 else 0 end as cng
from adubinsky.fp_v_ods_payment 
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
)sq1
)sq2
)sq3
)sq4
where eff_dt<exp_dt
)sq5
left join adubinsky.fp_dds_sat_user_prop s on s.user_key=sq5.user_key and s.exp_dt>sq5.eff_dt
where s.user_key is null
;
    """
)
dds_sat_payment_prop = PostgresOperator(
    task_id="dds_sat_payment_prop",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""

update adubinsky.fp_dds_sat_payment_prop s
set exp_dt = v.eff_dttm - interval '1 second'
from 
(select v.payment_key,min(v.eff_dt) as eff_dttm from
(
select 
payment_key,
payment_sum_rub,
payment_dttm,
load_dttm,
eff_dt,
exp_dt,
hashsum,
case when eff_dt = min(eff_dt) over(partition by payment_key) then 1 else 0 end as frow
from(
select 
payment_key,
payment_sum_rub,
payment_dttm,
load_dttm,
eff_dt,
lead(eff_dt,1,'2999-12-31') over (partition by payment_key order by scng) -interval '1 second' as exp_dt,
pay_prop_hashsum as hashsum 
from(
select distinct
payment_key,
payment_sum_rub,
payment_dttm,
pay_prop_hashsum,
load_dttm,
min(eff_dt) over (partition by payment_key,scng) as eff_dt,
scng
from(
select 
payment_key,
payment_sum_rub,
payment_dttm,
pay_prop_hashsum,
load_dttm,
eff_dt,
sum(cng) over (partition by payment_key order by rn) as scng
from(
select 
payment_key,
sum as payment_sum_rub,
pay_date as payment_dttm,
pay_prop_hashsum,
load_dttm,
pay_date as eff_dt,
row_number() over (partition by payment_key order by pay_date) as rn,
case when 
  lag(pay_prop_hashsum,1,pay_prop_hashsum) over (partition by payment_key order by pay_date)!=pay_prop_hashsum
  then 1 else 0 end as cng
from adubinsky.fp_v_ods_payment 
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
)sq1
)sq2
)sq3
)sq4
where eff_dt<exp_dt  
) v
join adubinsky.fp_dds_sat_payment_prop sv on v.payment_key=sv.payment_key and sv.exp_dt='2999-12-31' and v.hashsum!=sv.hashsum
group by v.payment_key)v
where s.payment_key=v.payment_key and s.exp_dt='2999-12-31'
;
insert into adubinsky.fp_dds_sat_payment_prop
(
payment_key,
payment_sum_rub,
payment_dttm,
load_dttm,
eff_dt,
exp_dt,
hashsum
)
select
sq5.payment_key,
sq5.payment_sum_rub,
sq5.payment_dttm,
sq5.load_dttm,
sq5.eff_dt,
sq5.exp_dt,
sq5.hashsum
from(
select 
payment_key,
payment_sum_rub,
payment_dttm,
load_dttm,
eff_dt,
exp_dt,
hashsum,
case when eff_dt = min(eff_dt) over(partition by payment_key) then 1 else 0 end as frow
from(
select 
payment_key,
payment_sum_rub,
payment_dttm,
load_dttm,
eff_dt,
lead(eff_dt,1,'2999-12-31') over (partition by payment_key order by scng) -interval '1 second' as exp_dt,
pay_prop_hashsum as hashsum 
from(
select distinct
payment_key,
payment_sum_rub,
payment_dttm,
pay_prop_hashsum,
load_dttm,
min(eff_dt) over (partition by payment_key,scng) as eff_dt,
scng
from(
select 
payment_key,
payment_sum_rub,
payment_dttm,
pay_prop_hashsum,
load_dttm,
eff_dt,
sum(cng) over (partition by payment_key order by rn) as scng
from(
select 
payment_key,
sum as payment_sum_rub,
pay_date as payment_dttm,
pay_prop_hashsum,
load_dttm,
pay_date as eff_dt,
row_number() over (partition by payment_key order by pay_date) as rn,
case when 
  lag(pay_prop_hashsum,1,pay_prop_hashsum) over (partition by payment_key order by pay_date)!=pay_prop_hashsum
  then 1 else 0 end as cng
from adubinsky.fp_v_ods_payment 
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
)sq1
)sq2
)sq3
)sq4
where eff_dt<exp_dt
)sq5
left join adubinsky.fp_dds_sat_payment_prop s on s.payment_key=sq5.payment_key and s.exp_dt>sq5.eff_dt
where s.payment_key is null
;
    """
)
dds_sat_issue_prop = PostgresOperator(
    task_id="dds_sat_issue_prop",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_sat_issue_prop s
set exp_dt = v.tech_dt - interval '1 second'
from adubinsky.fp_v_ods_issue v
where '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp = v.tech_dt 
and v.issue_prop_hashsum!=s.hashsum and v.issue_key=s.issue_key;

insert into adubinsky.fp_dds_sat_issue_prop
(
issue_key,
issue_start_dttm,
issue_end_dttm,
issue_title,
issue_note,
load_dttm,
eff_dt,
exp_dt,
hashsum
)
select distinct
v.issue_key,
v.issue_start_dt,
v.issue_end_dt,
v.issue_title,
v.issue_note,
v.load_dttm,
v.tech_dt as eff_dttm,
'2999-12-31 00:00:00'::timestamp as exp_dttm,
v.issue_prop_hashsum
from adubinsky.fp_v_ods_issue v
where '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp = v.tech_dt 
and not exists (select 1 from adubinsky.fp_dds_sat_issue_prop s where v.issue_key=s.issue_key and s.exp_dt='2999-12-31 00:00:00');

    """
)
dds_sat_billing_prop = PostgresOperator(
    task_id="dds_sat_billing_prop",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_sat_billing_prop s
set exp_dt = v.eff_dttm - interval '1 second'
from 
(select v.billing_key,min(v.eff_dt) as eff_dttm from
(
select 
billing_key,
billing_sum_rub,
billing_dttm,
load_dttm,
eff_dt,
exp_dt,
hashsum,
case when eff_dt = min(eff_dt) over(partition by billing_key) then 1 else 0 end as frow
from(
select 
billing_key,
billing_sum_rub,
billing_dttm,
load_dttm,
eff_dt,
lead(eff_dt,1,'2999-12-31') over (partition by billing_key order by scng) -interval '1 second' as exp_dt,
billing_prop_hashsum as hashsum 
from(
select distinct
billing_key,
billing_sum_rub,
billing_dttm,
billing_prop_hashsum,
load_dttm,
min(eff_dt) over (partition by billing_key,scng) as eff_dt,
scng
from(
select 
billing_key,
billing_sum_rub,
billing_dttm,
billing_prop_hashsum,
load_dttm,
eff_dt,
sum(cng) over (partition by billing_key order by rn) as scng
from(
select 
billing_key,
sum as billing_sum_rub,
created_at as billing_dttm,
billing_prop_hashsum,
load_dttm,
created_at as eff_dt,
row_number() over (partition by billing_key order by created_at) as rn,
case when 
  lag(billing_prop_hashsum,1,billing_prop_hashsum) over (partition by billing_key order by created_at)!=billing_prop_hashsum
  then 1 else 0 end as cng
from adubinsky.fp_v_ods_billing 
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
)sq1
)sq2
)sq3
)sq4
where eff_dt<exp_dt  
) v
join adubinsky.fp_dds_sat_billing_prop sv on v.billing_key=sv.billing_key and sv.exp_dt='2999-12-31' and v.hashsum!=sv.hashsum
group by v.billing_key)v
where s.billing_key=v.billing_key and s.exp_dt='2999-12-31'
;
insert into adubinsky.fp_dds_sat_billing_prop
(
billing_key,
billing_sum_rub,
billing_dttm,
load_dttm,
eff_dt,
exp_dt,
hashsum
)
select
sq5.billing_key,
sq5.billing_sum_rub,
sq5.billing_dttm,
sq5.load_dttm,
sq5.eff_dt,
sq5.exp_dt,
sq5.hashsum
from(
select 
billing_key,
billing_sum_rub,
billing_dttm,
load_dttm,
eff_dt,
exp_dt,
hashsum,
case when eff_dt = min(eff_dt) over(partition by billing_key) then 1 else 0 end as frow
from(
select 
billing_key,
billing_sum_rub,
billing_dttm,
load_dttm,
eff_dt,
lead(eff_dt,1,'2999-12-31') over (partition by billing_key order by scng) -interval '1 second' as exp_dt,
billing_prop_hashsum as hashsum 
from(
select distinct
billing_key,
billing_sum_rub,
billing_dttm,
billing_prop_hashsum,
load_dttm,
min(eff_dt) over (partition by billing_key,scng) as eff_dt,
scng
from(
select 
billing_key,
billing_sum_rub,
billing_dttm,
billing_prop_hashsum,
load_dttm,
eff_dt,
sum(cng) over (partition by billing_key order by rn) as scng
from(
select 
billing_key,
sum as billing_sum_rub,
created_at as billing_dttm,
billing_prop_hashsum,
load_dttm,
created_at as eff_dt,
row_number() over (partition by billing_key order by created_at) as rn,
case when 
  lag(billing_prop_hashsum,1,billing_prop_hashsum) over (partition by billing_key order by created_at)!=billing_prop_hashsum
  then 1 else 0 end as cng
from adubinsky.fp_v_ods_billing 
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
)sq1
)sq2
)sq3
)sq4
where eff_dt<exp_dt
)sq5
left join adubinsky.fp_dds_sat_billing_prop s on s.billing_key=sq5.billing_key and s.exp_dt>sq5.eff_dt
where s.billing_key is null
    """
)
dds_sat_traffic_prop = PostgresOperator(
    task_id="dds_sat_traffic_prop",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_sat_traffic_prop s
set exp_dt = v.eff_dttm - interval '1 second'
from 
(select v.traffic_key,min(v.eff_dt) as eff_dttm from
(
select 
traffic_key,
traffic_dttm,
traffic_in_b,
traffic_out_b,
load_dttm,
eff_dt,
exp_dt,
hashsum,
case when eff_dt = min(eff_dt) over(partition by traffic_key) then 1 else 0 end as frow
from(
select 
traffic_key,
traffic_dttm,
traffic_in_b,
traffic_out_b,
load_dttm,
eff_dt,
lead(eff_dt,1,'2999-12-31') over (partition by traffic_key order by scng) -interval '1 second' as exp_dt,
traffic_prop_hashsum as hashsum 
from(
select distinct
traffic_key,
traffic_dttm,
traffic_in_b,
traffic_out_b,
traffic_prop_hashsum,
load_dttm,
min(eff_dt) over (partition by traffic_key,scng) as eff_dt,
scng
from(
select 
traffic_key,
traffic_dttm,
traffic_in_b,
traffic_out_b,
traffic_prop_hashsum,
load_dttm,
eff_dt,
sum(cng) over (partition by traffic_key order by rn) as scng
from(
select 
traffic_key,
traffic_dt as traffic_dttm,
traffic_in_b,
traffic_out_b,
traffic_prop_hashsum,
load_dttm,
traffic_dt as eff_dt,
row_number() over (partition by traffic_key order by traffic_dt) as rn,
case when 
  lag(traffic_prop_hashsum,1,traffic_prop_hashsum) over (partition by traffic_key order by traffic_dt)!=traffic_prop_hashsum
  then 1 else 0 end as cng
from adubinsky.fp_v_ods_traffic 
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
)sq1
)sq2
)sq3
)sq4
where eff_dt<exp_dt  
) v
join adubinsky.fp_dds_sat_traffic_prop sv on v.traffic_key=sv.traffic_key and sv.exp_dt='2999-12-31' and v.hashsum!=sv.hashsum
group by v.traffic_key)v
where s.traffic_key=v.traffic_key and s.exp_dt='2999-12-31'
;
insert into adubinsky.fp_dds_sat_traffic_prop
(
traffic_key,
traffic_dttm,
traffic_in_b,
traffic_out_b,
load_dttm,
eff_dt,
exp_dt,
hashsum
)
select
sq5.traffic_key,
sq5.traffic_dttm,
sq5.traffic_in_b,
sq5.traffic_out_b,
sq5.load_dttm,
sq5.eff_dt,
sq5.exp_dt,
sq5.hashsum
from(
select 
traffic_key,
traffic_dttm,
traffic_in_b,
traffic_out_b,
load_dttm,
eff_dt,
exp_dt,
hashsum,
case when eff_dt = min(eff_dt) over(partition by traffic_key) then 1 else 0 end as frow
from(
select 
traffic_key,
traffic_dttm,
traffic_in_b,
traffic_out_b,
load_dttm,
eff_dt,
lead(eff_dt,1,'2999-12-31') over (partition by traffic_key order by scng) -interval '1 second' as exp_dt,
traffic_prop_hashsum as hashsum 
from(
select distinct
traffic_key,
traffic_dttm,
traffic_in_b,
traffic_out_b,
traffic_prop_hashsum,
load_dttm,
min(eff_dt) over (partition by traffic_key,scng) as eff_dt,
scng
from(
select 
traffic_key,
traffic_dttm,
traffic_in_b,
traffic_out_b,
traffic_prop_hashsum,
load_dttm,
eff_dt,
sum(cng) over (partition by traffic_key order by rn) as scng
from(
select 
traffic_key,
traffic_dt as traffic_dttm,
traffic_in_b,
traffic_out_b,
traffic_prop_hashsum,
load_dttm,
traffic_dt as eff_dt,
row_number() over (partition by traffic_key order by traffic_dt) as rn,
case when 
  lag(traffic_prop_hashsum,1,traffic_prop_hashsum) over (partition by traffic_key order by traffic_dt)!=traffic_prop_hashsum
  then 1 else 0 end as cng
from adubinsky.fp_v_ods_traffic 
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
)sq1
)sq2
)sq3
)sq4
where eff_dt<exp_dt
)sq5
left join adubinsky.fp_dds_sat_traffic_prop s on s.traffic_key=sq5.traffic_key and s.exp_dt>sq5.eff_dt
where s.traffic_key is null
    """
)
dds_sat_device = PostgresOperator(
    task_id="dds_sat_device",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_sat_device s
set exp_dt = v.eff_dttm - interval '1 second'
from 
(select v.device_key,min(v.eff_dt) as eff_dttm from
(
select 
device_key,
device_ip_addr,
load_dttm,
eff_dt,
exp_dt,
hashsum,
case when eff_dt = min(eff_dt) over(partition by device_key) then 1 else 0 end as frow
from(
select 
device_key,
device_ip_addr,
load_dttm,
eff_dt,
lead(eff_dt,1,'2999-12-31') over (partition by device_key order by scng) -interval '1 second' as exp_dt,
device_prop_hashsum as hashsum 
from(
select distinct
device_key,
device_ip_addr,
device_prop_hashsum,
load_dttm,
min(eff_dt) over (partition by device_key,scng) as eff_dt,
scng
from(
select 
device_key,
device_ip_addr,
device_prop_hashsum,
load_dttm,
eff_dt,
sum(cng) over (partition by device_key order by rn) as scng
from(
select 
device_key,
device_ip_addr,
device_prop_hashsum,
load_dttm,
traffic_dt as eff_dt,
row_number() over (partition by device_key order by traffic_dt) as rn,
case when 
  lag(device_prop_hashsum,1,device_prop_hashsum) over (partition by device_key order by traffic_dt)!=device_prop_hashsum
  then 1 else 0 end as cng
from adubinsky.fp_v_ods_traffic 
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
)sq1
)sq2
)sq3
)sq4
where eff_dt<exp_dt  
) v
join adubinsky.fp_dds_sat_device sv on v.device_key=sv.device_key and sv.exp_dt='2999-12-31' and v.hashsum!=sv.hashsum
group by v.device_key)v
where s.device_key=v.device_key and s.exp_dt='2999-12-31'
;
insert into adubinsky.fp_dds_sat_device
(
device_key,
device_ip_addr,
load_dttm,
eff_dt,
exp_dt,
hashsum
)
select
sq5.device_key,
sq5.device_ip_addr,
sq5.load_dttm,
sq5.eff_dt,
sq5.exp_dt,
sq5.hashsum
from(
select 
device_key,
device_ip_addr,
load_dttm,
eff_dt,
exp_dt,
hashsum,
case when eff_dt = min(eff_dt) over(partition by device_key) then 1 else 0 end as frow
from(
select 
device_key,
device_ip_addr,
load_dttm,
eff_dt,
lead(eff_dt,1,'2999-12-31') over (partition by device_key order by scng) -interval '1 second' as exp_dt,
device_prop_hashsum as hashsum 
from(
select distinct
device_key,
device_ip_addr,
device_prop_hashsum,
load_dttm,
min(eff_dt) over (partition by device_key,scng) as eff_dt,
scng
from(
select 
device_key,
device_ip_addr,
device_prop_hashsum,
load_dttm,
eff_dt,
sum(cng) over (partition by device_key order by rn) as scng
from(
select 
device_key,
device_ip_addr,
device_prop_hashsum,
load_dttm,
traffic_dt as eff_dt,
row_number() over (partition by device_key order by traffic_dt) as rn,
case when 
  lag(device_prop_hashsum,1,device_prop_hashsum) over (partition by device_key order by traffic_dt)!=device_prop_hashsum
  then 1 else 0 end as cng
from adubinsky.fp_v_ods_traffic 
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
)sq1
)sq2
)sq3
)sq4
where eff_dt<exp_dt
)sq5
left join adubinsky.fp_dds_sat_device s on s.device_key=sq5.device_key and s.exp_dt>sq5.eff_dt
where s.device_key is null
;
    """
)
dds_sat_billing_per = PostgresOperator(
    task_id="dds_sat_billing_per",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_sat_billing_per s
set exp_dt = v.tech_dt - interval '1 second'
from adubinsky.fp_v_ods_billing v
where '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp = v.tech_dt
and v.billing_per_hashsum!=s.hashsum and v.billing_per_key=s.billing_per_key;

update adubinsky.fp_dds_sat_billing_per s
set exp_dt = '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp - interval '1 second'
where 
not exists(select 1 from adubinsky.fp_v_ods_billing v where
'{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp = v.tech_dt
and v.billing_per_key=s.billing_per_key);

insert into adubinsky.fp_dds_sat_billing_per
(
billing_per_key,
billing_year,
billing_year_month,
load_dttm,
eff_dt,
exp_dt,
hashsum
)
select distinct
v.billing_per_key,
(regexp_replace(v.billing_period,'-','')::int/100)::int as billing_year,
regexp_replace(v.billing_period,'-','')::int as billing_year_month,
v.load_dttm,
v.tech_dt as eff_dt,
'2999-12-31 00:00:00'::timestamp as exp_dttm,
v.billing_per_hashsum
from adubinsky.fp_v_ods_billing v
where '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp = v.tech_dt
and not exists (select 1 from adubinsky.fp_dds_sat_billing_per s where v.billing_per_key=s.billing_per_key and s.exp_dt='2999-12-31 00:00:00');
    """
)
dds_sat_user_mdm = PostgresOperator(
    task_id="dds_sat_user_mdm",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_sat_user_mdm s
set exp_dt = v.eff_dttm - interval '1 second'
from adubinsky.fp_v_ods_mdm_user v
where '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp between v.eff_dttm and v.exp_dttm
and v.mdm_user_prop_hashsum!=s.hashsum and v.user_key=s.user_key;

update adubinsky.fp_dds_sat_user_mdm s
set exp_dt = '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp - interval '1 second'
where 
not exists(select 1 from adubinsky.fp_v_ods_mdm_user v where
'{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp between v.eff_dttm and v.exp_dttm
and v.user_key=s.user_key);

insert into adubinsky.fp_dds_sat_user_mdm
(
user_key,
user_reg_dt,
premial_type,
load_dttm,
eff_dt,
exp_dt,
hashsum
)
select distinct
v.user_key,
registered_at as user_reg_dt,
is_vip as premial_type,
v.load_dttm,
v.eff_dttm,
v.exp_dttm,
v.mdm_user_prop_hashsum
from adubinsky.fp_v_ods_mdm_user v
where '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp between v.eff_dttm and v.exp_dttm
and not exists (select 1 from adubinsky.fp_dds_sat_user_mdm s where v.user_key=s.user_key and s.exp_dt='2999-12-31 00:00:00');
    """
)
all_ods_loaded >>  dds_sat_reg_per
all_ods_loaded >>  dds_sat_user_prop
all_ods_loaded >>  dds_sat_payment_prop
all_ods_loaded >>  dds_sat_issue_prop
all_ods_loaded >>  dds_sat_billing_prop
all_ods_loaded >>  dds_sat_traffic_prop
all_ods_loaded >>  dds_sat_device
all_ods_loaded >>  dds_sat_billing_per
all_ods_loaded >>  dds_sat_user_mdm

#Блок загрузки линков DDS

dds_link_payment_users = PostgresOperator(
    task_id="dds_link_payment_users",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_link_payment_users l
set exp_dt= v.tech_dt - interval '1 second'
from adubinsky.fp_v_ods_payment v
where v.tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and v.payment_key=l.payment_key and (v.user_key!=l.user_key);

insert into adubinsky.fp_dds_link_payment_users
(
payment_key,
user_key,
load_dttm,
eff_dt,
exp_dt
)
select 
payment_key,
user_key,
load_dttm,
tech_dt as eff_dt,
'2999-12-31 00:00:00' as exp_dt
from adubinsky.fp_v_ods_payment v
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and not exists (select 1 from adubinsky.fp_dds_link_payment_users l
where l.payment_key=v.payment_key and l.user_key=v.user_key
and l.eff_dt<=v.tech_dt
)
;
    """
)
dds_link_issue_users = PostgresOperator(
    task_id="dds_link_issue_users",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_link_issue_users l
set exp_dt= v.tech_dt - interval '1 second'
from adubinsky.fp_v_ods_issue v
where v.tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and v.issue_key=l.issue_key and (v.user_key!=l.user_key);

insert into adubinsky.fp_dds_link_issue_users
(
issue_key,
user_key,
load_dttm,
eff_dt,
exp_dt
)
select 
issue_key,
user_key,
load_dttm,
tech_dt as eff_dt,
'2999-12-31 00:00:00' as exp_dt
from adubinsky.fp_v_ods_issue v
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and not exists (select 1 from adubinsky.fp_dds_link_issue_users l
where l.issue_key=v.issue_key and l.user_key=v.user_key
and l.eff_dt<=v.tech_dt
)
;

    """
)
dds_link_billing_users = PostgresOperator(
    task_id="dds_link_billing_users",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_link_billing_users l
set exp_dt= v.tech_dt - interval '1 second'
from adubinsky.fp_v_ods_billing v
where v.tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and v.billing_key=l.billing_key and (v.user_key!=l.user_key);

insert into adubinsky.fp_dds_link_billing_users
(
billing_key,
user_key,
load_dttm,
eff_dt,
exp_dt
)
select 
billing_key,
user_key,
load_dttm,
tech_dt as eff_dt,
'2999-12-31 00:00:00' as exp_dt
from adubinsky.fp_v_ods_billing v
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and not exists (select 1 from adubinsky.fp_dds_link_billing_users l
where l.billing_key=v.billing_key and l.user_key=v.user_key
and l.eff_dt<=v.tech_dt
)
;
    """
)
dds_link_traffic_users = PostgresOperator(
    task_id="dds_link_traffic_users",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_link_traffic_users l
set exp_dt= v.tech_dt - interval '1 second'
from adubinsky.fp_v_ods_traffic v
where v.tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and v.traffic_key=l.traffic_key and (v.user_key!=l.user_key);

insert into adubinsky.fp_dds_link_traffic_users
(
traffic_key,
user_key,
load_dttm,
eff_dt,
exp_dt
)
select 
traffic_key,
user_key,
load_dttm,
tech_dt as eff_dt,
'2999-12-31 00:00:00' as exp_dt
from adubinsky.fp_v_ods_traffic v
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and not exists (select 1 from adubinsky.fp_dds_link_traffic_users l
where l.traffic_key=v.traffic_key and l.user_key=v.user_key
and l.eff_dt<=v.tech_dt
)
;
    """
)
dds_link_device_users = PostgresOperator(
    task_id="dds_link_device_users",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_link_device_users s
set exp_dt = v.eff_dt - interval '1 second'
from 
(select v.device_key,min(v.eff_dt) as eff_dt from
(
select 
device_key,
load_dttm,
eff_dt,
exp_dt,
user_key,
case when eff_dt = min(eff_dt) over(partition by device_key) then 1 else 0 end as frow
from(
select 
device_key,
load_dttm,
eff_dt,
lead(eff_dt,1,'2999-12-31') over (partition by device_key order by scng) -interval '1 second' as exp_dt,
user_key as user_key 
from(
select distinct
device_key,
user_key,
load_dttm,
min(eff_dt) over (partition by device_key,scng) as eff_dt,
scng
from(
select 
device_key,
user_key,
load_dttm,
eff_dt,
sum(cng) over (partition by device_key order by rn) as scng
from(
select 
device_key,
user_key,
load_dttm,
traffic_dt as eff_dt,
row_number() over (partition by device_key order by traffic_dt) as rn,
case when 
  lag(user_key,1,user_key) over (partition by device_key order by traffic_dt)!=user_key
  then 1 else 0 end as cng
from adubinsky.fp_v_ods_traffic 
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
)sq1
)sq2
)sq3
)sq4
where eff_dt<exp_dt  
) v
join adubinsky.fp_dds_link_device_users sv on v.device_key=sv.device_key and sv.exp_dt='2999-12-31' and v.user_key!=sv.user_key
group by v.device_key)v
where s.device_key=v.device_key and s.exp_dt='2999-12-31'
;
insert into adubinsky.fp_dds_link_device_users
(
device_key,

load_dttm,
eff_dt,
exp_dt,
user_key
)
select
sq5.device_key,
sq5.load_dttm,
sq5.eff_dt,
sq5.exp_dt,
sq5.user_key
from(
select 
device_key,

load_dttm,
eff_dt,
exp_dt,
user_key,
case when eff_dt = min(eff_dt) over(partition by device_key) then 1 else 0 end as frow
from(
select 
device_key,

load_dttm,
eff_dt,
lead(eff_dt,1,'2999-12-31') over (partition by device_key order by scng) -interval '1 second' as exp_dt,
user_key as user_key 
from(
select distinct
device_key,

user_key,
load_dttm,
min(eff_dt) over (partition by device_key,scng) as eff_dt,
scng
from(
select 
device_key,

user_key,
load_dttm,
eff_dt,
sum(cng) over (partition by device_key order by rn) as scng
from(
select 
device_key,

user_key,
load_dttm,
traffic_dt as eff_dt,
row_number() over (partition by device_key order by traffic_dt) as rn,
case when 
  lag(user_key,1,user_key) over (partition by device_key order by traffic_dt)!=user_key
  then 1 else 0 end as cng
from adubinsky.fp_v_ods_traffic 
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
)sq1
)sq2
)sq3
)sq4
where eff_dt<exp_dt
)sq5
left join adubinsky.fp_dds_link_device_users s on s.device_key=sq5.device_key and s.exp_dt>sq5.eff_dt
where s.device_key is null
    """
)
dds_link_tariff_users = PostgresOperator(
    task_id="dds_link_tariff_users",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
insert into adubinsky.fp_dds_link_tariff_users
		(
		tariff_key,
		user_key,
		load_dttm,
		eff_dt,
		exp_dt
		)
		select distinct
		tariff_key,
		user_key,
		load_dttm,
		tech_dt as eff_dt,
		tech_dt+interval '1 year' - interval '1 second' as exp_dt
		from adubinsky.fp_v_ods_billing v
		where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
		and not exists (
		  select 1 from adubinsky.fp_dds_link_tariff_users l where v.tariff_key=l.tariff_key and v.user_key=l.user_key 
		)
		;
    """
)
dds_link_service_users = PostgresOperator(
    task_id="dds_link_service_users",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
  	insert into adubinsky.fp_dds_link_service_users
		(
		service_key,
		user_key,
		load_dttm,
		eff_dt,
		exp_dt
		)
		select distinct
		service_key,
		user_key,
		load_dttm,
		tech_dt as eff_dt,
		tech_dt+interval '1 year' - interval '1 second' as exp_dt
		from adubinsky.fp_v_ods_billing v
		where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
		and not exists (
		  select 1 from adubinsky.fp_dds_link_service_users l where v.service_key=l.service_key and v.user_key=l.user_key 
		)
		;

    """
)
dds_link_billing_per_billing = PostgresOperator(
    task_id="dds_link_billing_per_billing",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_link_billing_per_billing l
set exp_dt= v.tech_dt - interval '1 second'
from adubinsky.fp_v_ods_billing v
where v.tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and v.billing_key=l.billing_key and (v.billing_per_key!=l.billing_per_key);

insert into adubinsky.fp_dds_link_billing_per_billing
(
billing_key,
billing_per_key,
load_dttm,
eff_dt,
exp_dt
)
select 
billing_key,
billing_per_key,
load_dttm,
tech_dt as eff_dt,
'2999-12-31 00:00:00' as exp_dt
from adubinsky.fp_v_ods_billing v
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and not exists (select 1 from adubinsky.fp_dds_link_billing_per_billing l
where l.billing_key=v.billing_key and l.billing_per_key=v.billing_per_key
and l.eff_dt<=v.tech_dt
)
;
    """
)
dds_link_payment_doc_type_payment = PostgresOperator(
    task_id="dds_link_payment_doc_type_payment",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_link_payment_doc_type_payment l
set exp_dt= v.tech_dt - interval '1 second'
from adubinsky.fp_v_ods_payment v
where v.tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and v.payment_key=l.payment_key and (v.payment_doc_type_key!=l.payment_doc_type_key);

insert into adubinsky.fp_dds_link_payment_doc_type_payment
(
payment_key,
payment_doc_type_key,
load_dttm,
eff_dt,
exp_dt
)
select 
payment_key,
payment_doc_type_key,
load_dttm,
tech_dt as eff_dt,
'2999-12-31 00:00:00' as exp_dt
from adubinsky.fp_v_ods_payment v
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and not exists (select 1 from adubinsky.fp_dds_link_payment_doc_type_payment l
where l.payment_key=v.payment_key and l.payment_doc_type_key=v.payment_doc_type_key
and l.eff_dt<=v.tech_dt
)
;
    """
)
dds_link_tariff_billing = PostgresOperator(
    task_id="dds_link_tariff_billing",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_link_tariff_billing l
set exp_dt= v.tech_dt - interval '1 second'
from adubinsky.fp_v_ods_billing v
where v.tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and v.billing_key=l.billing_key and (v.tariff_key!=l.tariff_key);

insert into adubinsky.fp_dds_link_tariff_billing
(
billing_key,
tariff_key,
load_dttm,
eff_dt,
exp_dt
)
select 
billing_key,
tariff_key,
load_dttm,
tech_dt as eff_dt,
'2999-12-31 00:00:00' as exp_dt
from adubinsky.fp_v_ods_billing v
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and not exists (select 1 from adubinsky.fp_dds_link_tariff_billing l
where l.billing_key=v.billing_key and l.tariff_key=v.tariff_key
and l.eff_dt<=v.tech_dt
)
;
    """
)
dds_link_service_billing = PostgresOperator(
    task_id="dds_link_service_billing",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_link_service_billing l
set exp_dt= v.tech_dt - interval '1 second'
from adubinsky.fp_v_ods_billing v
where v.tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and v.billing_key=l.billing_key and (v.service_key!=l.service_key);

insert into adubinsky.fp_dds_link_service_billing
(
billing_key,
service_key,
load_dttm,
eff_dt,
exp_dt
)
select 
billing_key,
service_key,
load_dttm,
tech_dt as eff_dt,
'2999-12-31 00:00:00' as exp_dt
from adubinsky.fp_v_ods_billing v
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and not exists (select 1 from adubinsky.fp_dds_link_service_billing l
where l.billing_key=v.billing_key and l.service_key=v.service_key
and l.eff_dt<=v.tech_dt
)
;
    """
)
dds_link_billing_per_payment = PostgresOperator(
    task_id="dds_link_billing_per_payment",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_link_billing_per_payment l
set exp_dt= v.tech_dt - interval '1 second'
from adubinsky.fp_v_ods_payment v
where v.tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and v.payment_key=l.payment_key and (v.billing_per_key!=l.billing_per_key);

insert into adubinsky.fp_dds_link_billing_per_payment
(
payment_key,
billing_per_key,
load_dttm,
eff_dt,
exp_dt
)
select 
payment_key,
billing_per_key,
load_dttm,
tech_dt as eff_dt,
'2999-12-31 00:00:00' as exp_dt
from adubinsky.fp_v_ods_payment v
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and not exists (select 1 from adubinsky.fp_dds_link_billing_per_payment l
where l.payment_key=v.payment_key and l.billing_per_key=v.billing_per_key
and l.eff_dt<=v.tech_dt
)
;
    """
)

dds_link_service_issue = PostgresOperator(
    task_id="dds_link_service_issue",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update adubinsky.fp_dds_link_service_issue l
set exp_dt= v.tech_dt - interval '1 second'
from adubinsky.fp_v_ods_issue v
where v.tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and v.issue_key=l.issue_key and (v.service_key!=l.service_key);

insert into adubinsky.fp_dds_link_service_issue
(
issue_key,
service_key,
load_dttm,
eff_dt,
exp_dt
)
select 
issue_key,
service_key,
load_dttm,
tech_dt as eff_dt,
'2999-12-31 00:00:00' as exp_dt
from adubinsky.fp_v_ods_issue v
where tech_dt='{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp
and not exists (select 1 from adubinsky.fp_dds_link_service_issue l
where l.issue_key=v.issue_key and l.service_key=v.service_key
and l.eff_dt<=v.tech_dt
)
;
    """
)

dds_link_mdm_user_user = PostgresOperator(
    task_id="dds_link_mdm_user_user",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
update  adubinsky.fp_dds_link_mdm_user_user l
set exp_dt=v.exp_dttm
from adubinsky.fp_v_ods_mdm_user v
where v.mdm_user_user_key=l.mdm_user_user_key and v.eff_dttm=l.eff_dt and v.exp_dttm!=l.exp_dt;

insert into adubinsky.fp_dds_link_mdm_user_user
(
mdm_user_user_key,
user_key,
segment_key,
distr_key,
reg_per_key,
billing_mode_key,
load_dttm,
eff_dt,
exp_dt
)
select 
mdm_user_user_key,
user_key,
segment_key,
distr_key,
reg_per_key,
billing_mode_key,
load_dttm,
eff_dttm,
exp_dttm
from adubinsky.fp_v_ods_mdm_user v
where '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp between v.eff_dttm and v.exp_dttm
and not exists (select 1 from adubinsky.fp_dds_link_mdm_user_user l where l.mdm_user_user_key=v.mdm_user_user_key
and v.eff_dttm=l.eff_dt
);
    """
)


all_ods_loaded >>  dds_link_payment_users
all_ods_loaded >>  dds_link_issue_users
all_ods_loaded >>  dds_link_billing_users
all_ods_loaded >>  dds_link_traffic_users
all_ods_loaded >>  dds_link_device_users
all_ods_loaded >>  dds_link_tariff_users
all_ods_loaded >>  dds_link_service_users
all_ods_loaded >>  dds_link_billing_per_billing
all_ods_loaded >>  dds_link_payment_doc_type_payment
all_ods_loaded >>  dds_link_tariff_billing
all_ods_loaded >>  dds_link_service_billing
all_ods_loaded >>  dds_link_billing_per_payment
all_ods_loaded >>  dds_link_service_issue
all_ods_loaded >>  dds_link_mdm_user_user


dm_tfct_agg_year = PostgresOperator(
    task_id="dm_tfct_agg_year",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
alter table adubinsky.fp_dm_tfct_agg_year truncate PARTITION "p{{execution_date.strftime('%Y')}}";
insert into adubinsky.fp_dm_tfct_agg_year
(
  year,  
  segment_key, 
  distr_key, 
  billing_mode_key, 
  reg_per_key, 
  is_premial_type, 
  billing_sum_rub_th,
  payment_sum_rub_th,
  issue_cnt,
  traffic_amount_pb,
  load_dttm,
  tech_dt
)

with billing as 
(
select 
link_billing_users.user_key,
sat_billing_per.billing_year, 
sum(sat_billing_prop.billing_sum_rub)/1000 as billing_sum_rub_th 
 from 
adubinsky.fp_dds_link_billing_users link_billing_users 
join adubinsky.fp_dds_hub_billing hub_billing  on hub_billing.billing_key=link_billing_users.billing_key
    and '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second' >= hub_billing.eff_dt
join adubinsky.fp_dds_sat_billing_prop sat_billing_prop on sat_billing_prop.billing_key=hub_billing.billing_key
  --and '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second' between sat_billing_prop.eff_dt and sat_billing_prop.exp_dt
  and sat_billing_prop.eff_dt between '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp and '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second'
join adubinsky.fp_dds_link_billing_per_billing link_billing_per_billing on link_billing_per_billing.billing_key=hub_billing.billing_key
  and '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second' between link_billing_per_billing.eff_dt and link_billing_per_billing.exp_dt
join adubinsky.fp_dds_sat_billing_per sat_billing_per on sat_billing_per.billing_per_key=link_billing_per_billing.billing_per_key
  and '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second' between sat_billing_per.eff_dt and sat_billing_per.exp_dt
where '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second' between link_billing_users.eff_dt and link_billing_users.exp_dt
group by 
link_billing_users.user_key,
sat_billing_per.billing_year
),
payment as 
(
select 
link_payment_users.user_key,
sat_billing_per.billing_year, 
sum(sat_payment_prop.payment_sum_rub)/1000 as payment_sum_rub_th
 from 
adubinsky.fp_dds_link_payment_users link_payment_users 
join adubinsky.fp_dds_hub_payment hub_payment  on hub_payment.payment_key=link_payment_users.payment_key
    and '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second' >= hub_payment.eff_dt
join adubinsky.fp_dds_sat_payment_prop sat_payment_prop on sat_payment_prop.payment_key=hub_payment.payment_key
  --and '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second' between sat_payment_prop.eff_dt and sat_payment_prop.exp_dt
  and sat_payment_prop.eff_dt between  '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp and  '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second'
join adubinsky.fp_dds_link_billing_per_payment link_billing_per_payment on link_billing_per_payment.payment_key=hub_payment.payment_key
  and '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second' between link_billing_per_payment.eff_dt and link_billing_per_payment.exp_dt
join adubinsky.fp_dds_sat_billing_per sat_billing_per on sat_billing_per.billing_per_key=link_billing_per_payment.billing_per_key
  and '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second' between sat_billing_per.eff_dt and sat_billing_per.exp_dt
where '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second' between link_payment_users.eff_dt and link_payment_users.exp_dt
group by 
link_payment_users.user_key,
sat_billing_per.billing_year
),
issue as
(
select 
link_issue_users.user_key,
'{{execution_date.strftime('%Y')}}'::int as billing_year,
count(distinct link_issue_users.issue_key)  as issue_cnt
 from 
adubinsky.fp_dds_link_issue_users link_issue_users 
join adubinsky.fp_dds_sat_issue_prop sat_issue_prop  on sat_issue_prop.issue_key=link_issue_users.issue_key
    and sat_issue_prop.issue_start_dttm  between '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp and '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second'
where '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second' between link_issue_users.eff_dt and link_issue_users.exp_dt
group by link_issue_users.user_key
),
traffic as
(
select 
link_traffic_users.user_key,
'{{execution_date.strftime('%Y')}}'::int as billing_year,
sum(traffic_in_b+traffic_out_b)/1024/1024/1024/1024 as traffic_pb
 from 
adubinsky.fp_dds_link_traffic_users link_traffic_users 
join adubinsky.fp_dds_sat_traffic_prop sat_traffic_prop  on sat_traffic_prop.traffic_key=sat_traffic_prop.traffic_key
    and sat_traffic_prop.eff_dt  between '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp and '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second'
where '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second' between link_traffic_users.eff_dt and link_traffic_users.exp_dt
group by link_traffic_users.user_key
),
calendar as
(
  select 
  distinct billing_year 
  from adubinsky.fp_dds_sat_billing_per
)
select 
billing_year,
segment_key,
distr_key,
billing_mode_key,
reg_per_key,
is_premial_type,
sum(billing_sum_rub_th) as billing_sum_rub_th,
sum(payment_sum_rub_th) as payment_sum_rub_th,
sum(issue_cnt) as issue_cnt,
sum(traffic_amount_pb) as traffic_amount_pb,
now() as load_dttm,
'{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp as tech_dt
from(
select 
coalesce(billing.billing_year,payment.billing_year,issue.billing_year,traffic.billing_year) as billing_year,
link_mdm_user_user.segment_key,
link_mdm_user_user.distr_key,
link_mdm_user_user.billing_mode_key,
link_mdm_user_user.reg_per_key,
sat_user_mdm.premial_type as is_premial_type,
coalesce(billing.billing_sum_rub_th,0)::numeric(38,2) as billing_sum_rub_th,
coalesce(payment.payment_sum_rub_th,0)::numeric(38,2) as payment_sum_rub_th,
coalesce(issue.issue_cnt,0)::numeric(38) as issue_cnt,
coalesce(traffic.traffic_pb,0)::numeric(38,2)  as traffic_amount_pb
from adubinsky.fp_dds_hub_users dds_hub_users
join calendar on 1=1
left join billing on billing.user_key=dds_hub_users.user_key and calendar.billing_year=billing.billing_year
left join payment on payment.user_key=dds_hub_users.user_key and calendar.billing_year=payment.billing_year
left join issue on issue.user_key=dds_hub_users.user_key and calendar.billing_year=issue.billing_year
left join traffic on traffic.user_key=dds_hub_users.user_key and calendar.billing_year=traffic.billing_year
join adubinsky.fp_dds_link_mdm_user_user link_mdm_user_user on link_mdm_user_user.user_key=dds_hub_users.user_key
  and '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second' between link_mdm_user_user.eff_dt and link_mdm_user_user.exp_dt
join adubinsky.fp_dds_sat_user_mdm sat_user_mdm on sat_user_mdm.user_key=dds_hub_users.user_key
  and '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second' between sat_user_mdm.eff_dt and sat_user_mdm.exp_dt
where '{{ execution_date.strftime("%Y-%m-%d")}}'::timestamp + interval '1 year' - interval '1 second' >=dds_hub_users.eff_dt
)sq1
where billing_year is not null
group by
billing_year,
segment_key,
distr_key,
billing_mode_key,
reg_per_key,
is_premial_type
;
    """
)

dm_dim_segment = PostgresOperator(
    task_id="dm_dim_segment",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
truncate table adubinsky.fp_dm_dim_segment;
insert into adubinsky.fp_dm_dim_segment
(
segment_key,
segment_name,
load_dttm
)
select segment_key, segment_nkey, now()
from adubinsky.fp_dds_hub_segment;
    """
)

dm_dim_distr = PostgresOperator(
    task_id="dm_dim_distr",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
truncate table adubinsky.fp_dm_dim_distr;
insert into adubinsky.fp_dm_dim_distr
(
distr_key,
distr_name,
load_dttm
)
select distr_key, distr_nkey, now()
from adubinsky.fp_dds_hub_distr;
    """
)

dm_dim_billing_mode = PostgresOperator(
    task_id="dm_dim_billing_mode",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
truncate table adubinsky.fp_dm_dim_billing_mode;
insert into adubinsky.fp_dm_dim_billing_mode
(
billing_mode_key,
billing_mode_name,
load_dttm
)
select billing_mode_key, billing_mode_nkey, now()
from adubinsky.fp_dds_hub_billing_mode;
    """
)

dm_dim_reg_per = PostgresOperator(
    task_id="dm_dim_reg_per",
    dag=dag,
    # postgres_conn_id=""postgres_default"",
    sql="""
truncate table adubinsky.fp_dm_dim_reg_per;
insert into adubinsky.fp_dm_dim_reg_per
(
reg_per_key,
reg_year,
reg_year_month,
load_dttm
)
select 
reg_per_key, 
reg_year,
reg_year_month, 
now()
from adubinsky.fp_dds_sat_reg_per
where exp_dt='2999-12-31 00:00:00'::timestamp
;
    """
)

dds_link_billing_users  >> dm_tfct_agg_year
dds_hub_billing  >> dm_tfct_agg_year
dds_sat_billing_prop  >> dm_tfct_agg_year
dds_link_billing_per_billing  >> dm_tfct_agg_year
dds_sat_billing_per  >> dm_tfct_agg_year
dds_link_payment_users  >> dm_tfct_agg_year
dds_hub_payment  >> dm_tfct_agg_year
dds_sat_payment_prop  >> dm_tfct_agg_year
dds_link_billing_per_payment  >> dm_tfct_agg_year
dds_sat_billing_per  >> dm_tfct_agg_year
dds_link_issue_users  >> dm_tfct_agg_year
dds_sat_issue_prop  >> dm_tfct_agg_year
dds_link_traffic_users  >> dm_tfct_agg_year
dds_sat_traffic_prop  >> dm_tfct_agg_year
dds_sat_billing_per  >> dm_tfct_agg_year
dds_hub_users  >> dm_tfct_agg_year
dds_link_mdm_user_user  >> dm_tfct_agg_year
dds_sat_user_mdm  >> dm_tfct_agg_year
dds_hub_segment  >> dm_dim_segment
dds_hub_distr  >> dm_dim_distr
dds_hub_billing_mode  >> dm_dim_billing_mode
dds_sat_reg_per  >>  dm_dim_reg_per
