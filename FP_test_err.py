
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
    USERNAME + '_fp_test_err',
    default_args = default_args,
    description = 'DWH ETL tasks',
    schedule_interval = "0 0 1 1 *",
)

all_ods_loaded = DummyOperator(task_id="all_ods_loaded", dag=dag)

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
