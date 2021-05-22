
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

all_ods_loaded = DummyOperator(task_id="all_ods_loaded", dag=dag)


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
