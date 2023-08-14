# AUDIT TRAIL: 1.0
# 1. Initial Version                                                VP 10-Apr-2023

from abc import ABC, abstractmethod
import re
from typing import List, Dict, Tuple, Callable, Any
from water.context.WaterContext import WaterContext # type: ignore
from datetime import datetime
from transformations.replication.CommonFunctions import CommonFunctions
from transformations.replication.DBFunctions import DBFunctions
from water.context.LocalWaterContext import LocalWaterContext
from water.clients.s3.LocalS3Client import LocalS3Client
from water.clients.s3.S3File import S3File
from water.models.TransformationJobInput import TransformationJobInput, LakeTablePath
from time import sleep
import pandas as pd
import numpy as np
from pandas import DataFrame
import json
import os
import time



class automation_script():
    def __init__(self,water_context: WaterContext):
        self.wc = water_context
        self.tenant_id = self.wc.job_input.tenantId

    def find_deadlocks(self):
        CommonFuncCall = CommonFunctions(water_context=self.wc)
        DBFuncCall = DBFunctions(water_context=self.wc)
        start_time = time.time()
        blocking_query = 'SELECT activity.pid,activity.usename,activity.query,blocking.pid AS blocking_id,blocking.query AS blocking_query,now()::timestamp(0) as event_time FROM pg_stat_activity AS activity JOIN pg_stat_activity AS blocking ON blocking.pid = ANY(pg_blocking_pids(activity.pid))'        
        df_stage_tables = pd.DataFrame()
        while time.time() - start_time < 3300:            
            df_stage_tables = df_stage_tables.append(self.wc.db_pandas_client.execute_select_query(blocking_query))
        df_stage_tables.to_csv(r'C:\Users\vineetp\Desktop\deadlock_df1.csv', index=False, header=True)

    def find_count_in_chg_tables(self):
        CommonFuncCall = CommonFunctions(water_context=self.wc)
        DBFuncCall = DBFunctions(water_context=self.wc)
        # start_time = time.time()
        chg_tbl_query = f'''select distinct change_table_name from ia_Admin.chgparm c where stage_table_name in (select table_name from ia_admin.insights_control_log icl where job_id='abd4fa14-1b53-439d-94dd-edb2c4020204')'''        
        df_stage_tables = self.wc.db_pandas_client.execute_select_query(chg_tbl_query)
        print(df_stage_tables)
        df_cnt_tables = pd.DataFrame()
        for chg_table in df_stage_tables['change_table_name']:
            query = f'''select '{chg_table}',count(*) from {chg_table}'''
            df_cnt_tables = df_cnt_tables.append(self.wc.db_pandas_client.execute_select_query(query))
        df_cnt_tables.to_csv(r'C:\Users\vineetp\Desktop\count_chgtbl_df.csv', index=False, header=True)

    def cdc_trigger_changes(self):
        CommonFuncCall = CommonFunctions(water_context=self.wc)
        DBFuncCall = DBFunctions(water_context=self.wc)
        # start_time = time.time()  
        chg_tbl_query = f'''select distinct * from ia_Admin.chgparm c where stage_table_schema='faismgr' except (select * from ia_Admin.chgparm c where pg_sql  like ('%(dsf_key)%') union select * from ia_Admin.chgparm c where pg_sql  like ('%JOIN%') union select * from ia_Admin.chgparm c where pg_sql  like ('%join%') UNION select * from ia_Admin.chgparm c where pg_sql  like ('%UNION%') union select * from ia_Admin.chgparm c where pg_sql  like ('%union%') union select * from ia_Admin.chgparm c where pg_sql  like ('%WITH%') union select * from ia_Admin.chgparm c where pg_sql  like ('%with%') union select * from ia_Admin.chgparm c where pg_sql  like ('%WHERE%') union select * from ia_Admin.chgparm c where pg_sql  like ('%where%')) '''        
        chg_query=""
        df_stage_tables = self.wc.db_pandas_client.execute_select_query(chg_tbl_query)
        df_cnt_tables = pd.DataFrame()
        for idx in df_stage_tables.index:
            # print(df_stage_tables['pg_sql'][idx])
            chg_query = df_stage_tables['pg_sql'][idx]
            union_query = (chg_query.replace('a.','rep.')).replace('rep.dml_type','a.dml_type')
            chg_query = chg_query + ' UNION ' + union_query + ' INNER JOIN ' + df_stage_tables['stage_table_schema'][idx]+'.'+df_stage_tables['stage_table_name'][idx] +' rep ON (dsf_key)'
            df_stage_tables['pg_sql'][idx] = chg_query
            # print(df_stage_tables['pg_sql'][idx])
            # print()
        self.wc.db_pandas_client.write_to_table(df_stage_tables,'ia_admin','chgparm_bkp') 
        self.wc.db_client.commit_only() 
        xml_script=''
        dict = {'SELECT':'SELECT &10;',',':',&10;','FROM':'&10;FROM','UNION':'&10;UNION&10;','INNER JOIN':'&#10;INNER JOIN&#10;'}
        for idx in df_stage_tables.index:
            if(df_stage_tables['pg_sql'][idx].__contains__('\r')):
                # chg_query = df_stage_tables['pg_sql'][idx].replace("\n"," ")
                chg_query = df_stage_tables['pg_sql'][idx].replace("\r\n"," ") 
            else:
                chg_query = df_stage_tables['pg_sql'][idx].replace("\n"," ")
          
            for key, value in dict.items():
                # print(df_stage_tables['pg_sql'][idx])
                chg_query = chg_query.replace(key,value)
            xml_script = xml_script + f"""		<update
			schemaName="ia_admin"
			tableName="chgparm">
			<column name="pg_sql" value="{chg_query}"/>
		  <where>stage_table_schema='{df_stage_tables['stage_table_schema'][idx]}' and stage_table_name='{df_stage_tables['stage_table_name'][idx]}' and target_constant='{df_stage_tables['target_constant'][idx]}'</where>
		</update>
"""
        f = open('C:\\Users\\vineetp\\Desktop\\trigger_changes_automation\\chgparm_faismgr.xml', 'w')
        f.write(xml_script)
        print(chg_query)
        print(xml_script)
        print()

            # break
        #     df_cnt_tables = df_cnt_tables.append(self.wc.db_pandas_client.execute_select_query(query))
        # df_cnt_tables.to_csv(r'C:\Users\vineetp\Desktop\count_chgtbl_df.csv', index=False, header=True)
