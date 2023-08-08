def f_load_to_db(
    otm_creds: dict, klad_creds: dict, month_d: str, i: int, cur_dict: dict
) -> None:
    """
    function gets response with given m_code and month and writes it to datbase
    """

    import json
    import logging

    import pandas as pd
    import requests
    from requests_ntlm import HttpNtlmAuth
    from sqlalchemy import create_engine, text

    print(f"current month is {month_d}, current mcode is {i}")
    try:  # конектимся к api
        source_url = f"http://10.50.31.154:8000/api/otmCdu?doCode={i}&month={month_d}"
        session = requests.Session()
        session.auth = HttpNtlmAuth(
            otm_creds["otm_api_login"], otm_creds["otm_api_pass"], session
        )
        resp = session.get(source_url)
        logging.info(f"[{i}] Successfully connected to API")
    except Exception as e:
        logging.error(f"[{i}] Connection to API failed with error: {e}")

    jsn = resp.text
    json_dataframe = pd.read_json(jsn)
    json_dataframe2 = json_dataframe.mask(json_dataframe == "")  # замена '' на NaN

    cur_dict["klad"].execute(
        """select column_name from klad_conf.src_data_tab_column tdtc 
where src_data_table_id = 1427153 order by ordinal_position"""
    )

    cl = cur_dict["klad"].fetchall()
    column_list = []
    for column in cl:
        column_list.append(column[0])  # получаем список колонок в текущей tgt версии

    drop_column_list = []  # список колонок на удаление
    for column in list(json_dataframe2.columns):
        if column not in column_list:
            drop_column_list.append(column)

    json_dataframe2["do_code"] = i
    json_dataframe2["month_d"] = month_d

    json_dataframe2.drop(columns=drop_column_list, inplace=True)

    engine = create_engine(
        f"postgresql://{klad_creds['db_login']}:{klad_creds['db_pass']}@{klad_creds['db_host']}:5432/edw",
        connect_args={"options": "-csearch_path={}".format("ods_otm_src")},
    )

    with engine.connect() as connection:
        json_dataframe2.to_sql(
            name="otm_stg", con=connection, if_exists="append", index=False
        )
