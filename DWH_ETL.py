from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'adubinsky'
#установил зависимости в расчетах для корректного заполнения и учета историчности и версионности
#т.к. в ДЗ номер 3 была ошибка при загруке и все даты платежей у меня были в 2013 году, а перегрузить данные мне уже не удалось
#написал письмо с этим вопросом, но видимо пока его не успели посмотреть
#реализован запуск заполнения данных помесячно
default_args = {
    'owner': USERNAME,
    'depends_on_past' : True,
     'wait_for_downstream' : True,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_dwh_etl2',
    default_args = default_args,
    description = 'DWH ETL tasks',
    schedule_interval = "0 0 1 * *",
)
#очистка партции на ODS
ods_payment_trunc = PostgresOperator(
    task_id="ods_payment_trunc",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
      alter table adubinsky.ods_payment truncate PARTITION "{{execution_date.strftime('%Y%m')}}";
    """
    )
#Заполнение очищенной на прошлом шаге партиции
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
        where pay_date between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 month' - interval '1 second';
    """
)

ods_payment_trunc>>ods_payment

all_ods_loaded = DummyOperator(task_id="all_ods_loaded", dag=dag)

ods_payment >> all_ods_loaded

#Загрузка хабов
#т.к. хабы не имеют изменяющихся атрибутов, а просто одно поле ключ в натуральном и хешированном виде
#то проводится простой инсерт новых записей
dds_hub_payment = PostgresOperator(
    task_id="dds_hub_payment",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        insert into adubinsky.dds_hub_payment
			(
			  pay_hkey , 
			  pay_key , 
			  load_dt  , 
			  eff_dt , 
			  exp_dt 
			)
			select pay_hkey,
			pay_key,
			 now(),
			  '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP,
			  '2999-12-31 00:00:00' ::TIMESTAMP
			  from
			(select 
			v.pay_hkey , 
			  v.pay_key,
			  row_number() over (partition by v.pay_hkey order by v.pay_date desc) rn
			  FROM adubinsky.v_dds_payment v
			  left join adubinsky.dds_hub_payment d on v.pay_hkey=d.pay_hkey
			where pay_date between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 month' - interval '1 second'
			and d.pay_hkey is null) t
			where t.rn=1
			;
    """
)

dds_hub_billing_period = PostgresOperator(
    task_id="dds_hub_billing_period",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        insert into adubinsky.dds_hub_billing_period
				(
				  billing_period_hkey , 
				  billing_period_key , 
				  load_dt  , 
				  eff_dt , 
				  exp_dt 
				)
				select billing_period_hkey,
				billing_period_key,
				 now(),
				  '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP,
				  '2999-12-31 00:00:00' ::TIMESTAMP
				  from
				(select 
				v.billing_period_hkey , 
				  v.billing_period_key,
				  row_number() over (partition by v.billing_period_hkey order by v.pay_date desc) rn
				  FROM adubinsky.v_dds_billing_period v
				  left join adubinsky.dds_hub_billing_period d on v.billing_period_hkey=d.billing_period_hkey
				where pay_date between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 month' - interval '1 second'
				and d.billing_period_hkey is null) t
				where t.rn=1
				;
    """
)

dds_hub_user = PostgresOperator(
    task_id="dds_hub_user",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
			insert into adubinsky.dds_hub_user
			(
			  user_hkey , 
			  user_key , 
			  load_dt  , 
			  eff_dt , 
			  exp_dt 
			)
			select user_hkey,
			user_key,
			 now(),
			  '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP,
			  '2999-12-31 00:00:00' ::TIMESTAMP
			  from
			(select 
			v.user_hkey , 
			  v.user_key,
			  row_number() over (partition by v.user_hkey order by v.pay_date desc) rn
			  FROM adubinsky.v_dds_user v
			  left join adubinsky.dds_hub_user d on v.user_hkey=d.user_hkey
			where pay_date between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 month' - interval '1 second'
			and d.user_hkey is null) t
			where t.rn=1
			;
    """
)

dds_hub_account = PostgresOperator(
    task_id="dds_hub_account",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
			insert into adubinsky.dds_hub_account
			(
			  account_hkey , 
			  account_key , 
			  load_dt  , 
			  eff_dt , 
			  exp_dt 
			)
			select account_hkey,
			account_key,
			 now(),
			  '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP,
			  '2999-12-31 00:00:00' ::TIMESTAMP
			  from
			(select 
			v.account_hkey , 
			  v.account_key,
			  row_number() over (partition by v.account_hkey order by v.pay_date desc) rn
			  FROM adubinsky.v_dds_account v
			  left join adubinsky.dds_hub_account d on v.account_hkey=d.account_hkey
			where pay_date between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 month' - interval '1 second'
			and d.account_hkey is null) t
			where t.rn=1
			;
    """
)

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)

all_ods_loaded >>  dds_hub_payment >> all_hubs_loaded
all_ods_loaded >>  dds_hub_billing_period >> all_hubs_loaded
all_ods_loaded >>  dds_hub_payment >> all_hubs_loaded
all_ods_loaded >>  dds_hub_account >> all_hubs_loaded

#загрузка линк таблиц
#т.к. в них имеемя версионность, то сначала проверяется не изменился ли связанный ключ и если да - то закрывается версия связи и заводится новая
#в задаче принял что один пользователь в периоде имеет только одни аккаунт и у одного платежа может быть один пользователь (в общем связи один ко многим)
dds_lnk_user_account = PostgresOperator(
    task_id="dds_lnk_user_account",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
			update adubinsky.dds_lnk_user_account d
			set exp_dt= '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP - interval '1 second'
			from 
			(
			select user_hkey, account_hkey,
			row_number() over (partition by user_hkey order by pay_date desc) rn
			 from adubinsky.v_dds_user
			where pay_date between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 month' - interval '1 second'
			) v
			where v.rn=1 and v.user_hkey=d.user_hkey and v.account_hkey!=d.account_hkey
			  and d.eff_dt>='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP;

			insert into adubinsky.dds_lnk_user_account
			(
			  user_hkey , 
			  account_hkey , 
			  load_dt  , 
			  eff_dt , 
			  exp_dt 
			)
			select user_hkey,
			account_hkey,
			 now(),
			  '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP,
			  '2999-12-31 00:00:00' ::TIMESTAMP
			  from
			(select 
			v.user_hkey , 
			  v.account_hkey,
			  row_number() over (partition by v.user_hkey order by v.pay_date desc) rn
			  FROM adubinsky.v_dds_user v
			  left join adubinsky.dds_lnk_user_account d on v.user_hkey=d.user_hkey and d.eff_dt>='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP
			where pay_date between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 month' - interval '1 second'
			and d.user_hkey is null) t
			where t.rn=1
			;
    """
)

dds_lnk_payment_user = PostgresOperator(
    task_id="dds_lnk_payment_user",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
		update adubinsky.dds_lnk_payment_user d
		set exp_dt= '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP - interval '1 second'
		from 
		(
		select pay_hkey, user_hkey,
		row_number() over (partition by pay_hkey order by pay_date desc) rn
		 from adubinsky.v_dds_payment
		where pay_date between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 month' - interval '1 second'
		) v
		where v.rn=1 and v.pay_hkey=d.pay_hkey and v.user_hkey!=d.user_hkey
		  and d.eff_dt>='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP;

		insert into adubinsky.dds_lnk_payment_user
		(
		  pay_hkey , 
		  user_hkey , 
		  load_dt  , 
		  eff_dt , 
		  exp_dt 
		)
		select pay_hkey,
		user_hkey,
		 now(),
		  '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP,
		  '2999-12-31 00:00:00' ::TIMESTAMP
		  from
		(select 
		v.pay_hkey , 
		  v.user_hkey,
		  row_number() over (partition by v.pay_hkey order by v.pay_date desc) rn
		  FROM adubinsky.v_dds_payment v
		  left join adubinsky.dds_lnk_payment_user d on v.pay_hkey=d.pay_hkey and d.eff_dt>='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP
		where pay_date between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 month' - interval '1 second'
		and d.pay_hkey is null) t
		where t.rn=1
		;
    """
)

dds_lnk_payment_billing = PostgresOperator(
    task_id="dds_lnk_payment_billing",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
		update adubinsky.dds_lnk_payment_billing d
		set exp_dt= '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP - interval '1 second'
		from 
		(
		select pay_hkey, billing_period_hkey,
		row_number() over (partition by pay_hkey order by pay_date desc) rn
		 from adubinsky.v_dds_payment
		where pay_date between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 month' - interval '1 second'
		) v
		where v.rn=1 and v.pay_hkey=d.pay_hkey and v.billing_period_hkey!=d.billing_period_hkey
		  and d.eff_dt>='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP;

		insert into adubinsky.dds_lnk_payment_billing
		(
		  pay_hkey , 
		  billing_period_hkey , 
		  load_dt  , 
		  eff_dt , 
		  exp_dt 
		)
		select pay_hkey,
		billing_period_hkey,
		 now(),
		  '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP,
		  '2999-12-31 00:00:00' ::TIMESTAMP
		  from
		(select 
		v.pay_hkey , 
		  v.billing_period_hkey,
		  row_number() over (partition by v.pay_hkey order by v.pay_date desc) rn
		  FROM adubinsky.v_dds_payment v
		  left join adubinsky.dds_lnk_payment_billing d on v.pay_hkey=d.pay_hkey and d.eff_dt>='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP
		where pay_date between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 month' - interval '1 second'
		and d.pay_hkey is null) t
		where t.rn=1
		;
    """
)


all_link_loaded = DummyOperator(task_id="all_link_loaded", dag=dag)

all_hubs_loaded >>  dds_lnk_user_account >> all_link_loaded
all_hubs_loaded >>  dds_lnk_payment_user >> all_link_loaded
all_hubs_loaded >>  dds_lnk_payment_billing >> all_link_loaded

#схоже с линк таблицами, только проверка идет по сумме хешей
dds_sat_user_inf = PostgresOperator(
    task_id="dds_sat_user_inf",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
		update adubinsky.dds_sat_user_inf d
		set exp_dt= '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP - interval '1 second'
		from 
		(
		select user_hkey, user_hashsumm,
		row_number() over (partition by user_hkey order by pay_date desc) rn
		 from adubinsky.v_dds_user
		where pay_date between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 month' - interval '1 second'
		) v
		where v.rn=1 and v.user_hkey=d.user_hkey and v.user_hashsumm!=d.user_hashsumm
		  and d.eff_dt>='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP;

		insert into adubinsky.dds_sat_user_inf
		(
		  user_hkey , 
		  phone , 
		  user_hashsumm ,
		  load_dt  , 
		  eff_dt , 
		  exp_dt 
		)
		select user_hkey,
		phone,
		user_hashsumm,
		 now(),
		  '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP,
		  '2999-12-31 00:00:00' ::TIMESTAMP
		  from
		(select 
		v.user_hkey , 
		v.phone,
		  v.user_hashsumm,
		  row_number() over (partition by v.user_hkey order by v.pay_date desc) rn
		  FROM adubinsky.v_dds_user v
		  left join adubinsky.dds_sat_user_inf d on v.user_hkey=d.user_hkey and d.eff_dt>='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP
		where pay_date between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 month' - interval '1 second'
		and d.user_hkey is null) t
		where t.rn=1
		;
        """
)
dds_sat_payment_inf = PostgresOperator(
    task_id="dds_sat_payment_inf",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
			update adubinsky.dds_sat_payment_inf d
			set exp_dt= '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP - interval '1 second'
			from 
			(
			select pay_hkey, payment_hashsumm,
			row_number() over (partition by pay_hkey order by pay_date desc) rn
			 from adubinsky.v_dds_payment
			where pay_date between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 month' - interval '1 second'
			) v
			where v.rn=1 and v.pay_hkey=d.pay_hkey and v.payment_hashsumm!=d.payment_hashsumm
			  and d.eff_dt>='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP;

			insert into adubinsky.dds_sat_payment_inf
			(
			  pay_hkey , 
			  pay_doc_type , 
			  pay_doc_num , 
			  pay_date  , 
			  payment_hashsumm ,
			  load_dt  , 
			  eff_dt , 
			  exp_dt 
			)
			select pay_hkey,
			pay_doc_type , 
			  pay_doc_num , 
			  pay_date  , 
			payment_hashsumm,
			 now(),
			  '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP,
			  '2999-12-31 00:00:00' ::TIMESTAMP
			  from
			(select 
			v.pay_hkey , 
			v.pay_doc_type , 
			  v.pay_doc_num , 
			  v.pay_date  , 
			  v.payment_hashsumm,
			  row_number() over (partition by v.pay_hkey order by v.pay_date desc) rn
			  FROM adubinsky.v_dds_payment v
			  left join adubinsky.dds_sat_payment_inf d on v.pay_hkey=d.pay_hkey and d.eff_dt>='{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP
			where v.pay_date between '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  and '{{ execution_date.strftime("%Y-%m-%d")}}'::TIMESTAMP  + interval '1 month' - interval '1 second'
			and d.pay_hkey is null) t
			where t.rn=1
			;
    """
)


all_sat_loaded = DummyOperator(task_id="all_sat_loaded", dag=dag)

all_link_loaded >>  dds_sat_user_inf >> all_sat_loaded
all_link_loaded >>  dds_sat_payment_inf >> all_sat_loaded


