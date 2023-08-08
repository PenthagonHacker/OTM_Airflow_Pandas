def main(otm_creds: dict, klad_creds: dict) -> None:
    import datetime
    import os
    import logging
    from sys import exit
    import psycopg2
    from otm_pandas.load_to_db import f_load_to_db

    do_code = [
        "OM0036",
        "OM0037",
        "OM0038",
        "OM0045",
        "OM0048",
        "OM0055",
        "OM0062",
        "OM0655",
    ]


    d = datetime.datetime.now()
    month_d = d.strftime("%Y%m")  # '202302'
    current_script_file_path = os.path.dirname(os.path.abspath(__file__))


    cur_dict = {}  # в словаре храним объекты курсоров

    for dbname in ["edw", "klad"]:  # конектимся к бд
        try:
            conn = psycopg2.connect(
                user=klad_creds["db_login"],
                dbname=dbname,
                host=klad_creds["db_host"],
                port=5432,
                password=klad_creds["db_pass"],
            )
            cur_dict[dbname] = conn.cursor()
            logging.info(f"Successfully connected to database {dbname}")

        except Exception as e:
            logging.error(f"Error! Could not connect to database: {e}")


    cur_dict["edw"].execute(
        """
    select count(1) from
    (select * from ods_otm_src.otm where write_ts <=  now() - INTERVAL '2 weeks' limit 1)q
    """
    )  # проверяем прошло ли две недели с последней перезагрузки таблицы

    test = cur_dict["edw"].fetchone()[0]

    if test == 1:  # две недели прошли
        cur_dict["edw"].execute("delete from ods_otm_src.otm_stg;")
        cur_dict["edw"].connection.commit()
        cur_dict["edw"].execute(
            "select DISTINCT month_d from ods_otm_src.otm order by month_d;"
        )  # список месяцев для перегрузки
        result = [month[0] for month in cur_dict["edw"].fetchall()]

        for month_d in result:
            for i in do_code:
                try:
                    f_load_to_db(otm_creds, klad_creds, month_d, i, cur_dict)
                except Exception as e:
                    logging.error(f"{str(e)}")

        try:
            cur_dict["edw"].execute("delete from ods_otm_src.otm;")
            cur_dict["edw"].connection.commit()
        except Exception as e:
            logging.error(f"{str(e)}")

        try:
            cur_dict["edw"].execute(
                "INSERT INTO ods_otm_src.otm SELECT * FROM ods_otm_src.otm_stg;"
            )
            cur_dict["edw"].connection.commit()
        except Exception as e:
            logging.error(f"{str(e)}")


    else:  # две недели не прошли
        print('# две недели не прошли')
        cur_dict["edw"].execute(f"delete from ods_otm_src.otm_stg;")
        cur_dict["edw"].connection.commit()

        for i in do_code:
            try:
                f_load_to_db(otm_creds, klad_creds, month_d, i, cur_dict)
            except Exception as e:
                logging.error(f"{str(e)}")
        try:
            cur_dict["edw"].execute(
                f"delete from ods_otm_src.otm where month_d = '{month_d}';"
            )
            cur_dict["edw"].connection.commit()
        except Exception as e:
            logging.error(f"{str(e)}")

        try:
            cur_dict["edw"].execute(
                "INSERT INTO ods_otm_src.otm SELECT * FROM ods_otm_src.otm_stg;"
            )
            cur_dict["edw"].connection.commit()
        except Exception as e:
            logging.error(f"{str(e)}")
            
    cur_dict["edw"].close()
    cur_dict["klad"].close()
    cur_dict["edw"].connection.close()




from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook

import datetime


connection_otm_api_creds = BaseHook.get_connection('OTM_API_CREDS')
connection_db = BaseHook.get_connection('gp_klad')


otm_creds = {
    "otm_api_login": connection_otm_api_creds.login,
    "otm_api_pass": connection_otm_api_creds.password,
}

klad_creds = {
    "db_login": connection_db.login,
    "db_pass": connection_db.password,
    "db_host": connection_db.host,
}



with DAG(
    'otm_pandas',
    start_date = datetime.datetime(2023, 8, 8),
    schedule_interval = '0 5 * * *',
    catchup = False,
    max_active_runs = 1
   
) as dag:

    process = PythonOperator(
        task_id='process_task',        
        provide_context=True,        
        python_callable=main,
        op_args = [otm_creds, klad_creds], 
    )

process