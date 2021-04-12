from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

USERNAME = 'adubinsky'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_data_lake_etl2',
    default_args = default_args,
    description = 'Data Lake ETL tasks',
    schedule_interval = "0 0 1 1 *",
)

ods_dict_part = {'ods_billing': ['cast(user_id as BIGINT), billing_period, service, tariff, CAST(sum as FLOAT), CAST(created_at as TIMESTAMP)',
                          'stg_billing', 'created_at'],
          'ods_payment': [
              'cast(user_id as BIGINT), pay_doc_type, CAST(pay_doc_num as BIGINT), account, phone, billing_period, CAST(pay_date as TIMESTAMP), CAST(sum as FLOAT)',
              'stg_payment', 'pay_date'],
          'ods_traffic': [
              'cast(user_id as BIGINT), CAST(CAST(`timestamp` as BIGINT) as TIMESTAMP), device_id, device_ip_addr, CAST(bytes_sent as BIGINT), CAST(bytes_received as BIGINT)',
              'stg_traffic', 'CAST(CAST(`timestamp` as BIGINT) as TIMESTAMP)']
			  }
			  
ods_dict_nopart = {'ods_issue': [
              'CAST(user_id as BIGINT), CAST(start_time as TIMESTAMP), CAST(end_time as TIMESTAMP), title, description, service',
              'stg_issue']
			  }
			  
dm_dict = {'dm_traffic' : ['user_id, MAX(bytes_received), MIN(bytes_received), AVG(bytes_received)', 'ods_traffic', 'year']
			}		  

flist = [ods_dict_part, ods_dict_nopart, dm_dict]

params = {'current_year' : 2013, 'job_suffix': randint(0, 100000)}

while params['current_year'] <= 2021:
	i = 0
	while i < 1:
		if i == 0:
			for k in flist[i]:
				lvl0_proc = DataProcHiveOperator(
					task_id = k,
					dag = dag,
					query = """INSERT OVERWRITE TABLE adubinsky.{3} PARTITION (year={4})
					SELECT {0} FROM adubinsky.{1} WHERE year({2}) = {4};""".format(ods_dict_part[k][0], ods_dict_part[k][1],
																										ods_dict_part[k][2], k, params['current_year']),
					cluster_name = 'cluster-dataproc',
					job_name = USERNAME + '_{0}_{1}_{2}'.format(k, params['current_year'], params['job_suffix']),
					region = 'europe-west3'
				)
			i += 1
			if i == 1:
				for k in flist[i]:
					lvl1_proc = DataProcHiveOperator(
						task_id = k,
						dag = dag,
						query = """INSERT OVERWRITE TABLE adubinsky.{2}
						SELECT {0} FROM adubinsky.{1} WHERE 1 = 1;""".format(ods_dict_nopart[k][0], ods_dict_nopart[k][1], k),
						cluster_name = 'cluster-dataproc',
						job_name = USERNAME + '_{0}_{1}_{2}'.format(k, params['current_year'], params['job_suffix']),
						region = 'europe-west3'
					)
				i += 1
				if i == 2:
					for k in flist[i]:
						lvl2_proc = DataProcHiveOperator(
							task_id = k,
							dag = dag,
							query="""INSERT OVERWRITE TABLE adubinsky.{3} PARTITION (year={4})
								SELECT {0} FROM adubinsky.{1} WHERE year({2}) = {4} GROUP BY user_id;""".format(dm_dict['dm_traffic'][0], dm_dict['dm_traffic'][1],
																							  dm_dict['dm_traffic'][2], 'dm_traffic',
																							  params['current_year']),
							cluster_name = 'cluster-dataproc',
							job_name = USERNAME + '_{0}_{1}_{2}'.format(k, params['current_year'], params['job_suffix']),
							region = 'europe-west3'
						)
				lvl0_proc >> lvl1_proc >> lvl2_proc
	params['current_year'] += 1
