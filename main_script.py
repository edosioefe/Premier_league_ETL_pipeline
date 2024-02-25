import pandas as pd
from pandas.io.json import json_normalize
from sqlalchemy import create_engine
import psycopg2
import requests
import json
import configparser
import sys
import datetime 
import top_assisters_api_to_staging as ta
import top_scorers_api_to_staging as ts
import standings_api_to_staging as s
import fixtures_api_to_staging as f
import results_api_to_staging as r
#from export_module import ExportDf



# This function will be in the main script 
def staging_to_core(host, user, password, dbname, port):
    
    conn = psycopg2.connect(host=host, user=user,
                              password=password, 
                              dbname=dbname, port=port)
    if conn:     
        
        try:
            cur = conn.cursor()
            cur.execute("CALL core.sp_results_staging_to_core();")
            cur.execute("CALL core.sp_fixtures_staging_to_core();")
            cur.execute("CALL core.sp_standings_staging_to_core();")
            cur.execute("CALL core.sp_top_scorers_staging_to_core();")
            cur.execute("CALL core.sp_top_assisters_staging_to_core();")
            conn.commit();
            

        except:
            print('Staging to core failed.')
            conn.rollback();
            print('Core table inserts rolledback.')
            
        finally:
            for notice in conn.notices:
                print(notice)
            cur.close()
            print('Core cursor closed.')
            conn.close()
            print('Core connection closed')
    else:
        print('Couldnt connect to database to complete staging_to_core function.')


def truncate_staging_tables(host, user, password, dbname, port):
    conn = psycopg2.connect(host=host, user=user,
                              password=password, 
                              dbname=dbname, port=port)
    if conn:
        try:
        
            cur = conn.cursor()
            cur.execute("""truncate table staging."results" restart identity;""")
            cur.execute("""truncate table staging."fixtures" restart identity;""")
            cur.execute("""truncate table staging."standings" restart identity;""")
            cur.execute("""truncate table staging."top_scorers" restart identity;""")
            cur.execute("""truncate table staging."top_assisters" restart identity;""")

            conn.commit();
            print('Tables truncated')
        except psycopg2.DatabaseError as error:
            print(error)
            print('Tables truncate failed')
        finally:
        
            cur.close()
            print('Truncate cursor closed')
            conn.close()
            print('Truncate connection closed')
    else:
        print('Couldnt connect to database to complete truncate_staging_tables function.')

if __name__ == '__main__':

    config = configparser.ConfigParser()
    config.read('configuration.ini')

    #config variables
    header_api_key = config['CONFIG']['header_api_key']
    header_url = config['CONFIG']['header_url']
    league_id = config['CONFIG']['league_id']
    season = config['CONFIG']['season']
    top_assisters_url = config['TOP_ASSISTERS']['top_assisters_url']
    top_assisters_df_type = config['TOP_ASSISTERS']['df_type_ta']
    top_scorers_url = config['TOP_SCORERS']['top_scorers_url']
    top_scorers_df_type = config['TOP_SCORERS']['df_type_ts']
    standings_url = config['STANDINGS']['standings_url']
    standings_df_type = config['STANDINGS']['df_type_s']
    fixtures_url = config['FIXTURES']['fixtures_url']
    fixtures_df_type = config['FIXTURES']['df_type_f']
    fixtures_next_num = config['FIXTURES']['next_num_fixtures']
    results_url = config['RESULTS']['results_url']
    results_df_type = config['RESULTS']['df_type_r']
    
    to_staging_string = config['DATABASE']['to_staging_string']
    core_host = config['DATABASE']['to_core_string_host'] 
    core_user = config['DATABASE']['to_core_string_user'] 
    core_password = config['DATABASE']['to_core_string_password'] 
    core_dbname = config['DATABASE']['to_core_string_dbname'] 
    core_port = config['DATABASE']['to_core_string_port']

#data injestion, transformation and load to staging tables of top assisters
    ta_json_data = ta.get_top_assisters_json(header_url, top_assisters_url, header_api_key, season, league_id, requests, sys, json)
    ta_df = ta.top_assisters_transformation(ta_json_data, pd)
    ta_ans = ta.top_assisters_to_staging(to_staging_string, ta_df, top_assisters_df_type, create_engine)

#data injestion, transformation and load to staging tables of top scorers
    ts_json_data = ts.get_top_scorers_json(header_url, top_scorers_url, header_api_key, season, league_id, requests, sys, json)
    ts_df = ts.top_scorers_transformation(ts_json_data, pd)
    ts_ans = ts.top_scorers_to_staging(to_staging_string, ts_df, top_scorers_df_type, create_engine)

#data injestion, transformation and load to staging tables of league standings
    s_json_data = s.get_standings_json(header_url, standings_url, header_api_key, season, league_id, requests, sys, json)
    s_df = s.standings_transformation(s_json_data, pd)
    s_ans = s.standings_to_staging(to_staging_string, s_df, standings_df_type, create_engine)

#data injestion, transformation and load to staging tables of upcoming fixtures
    f_json_data = f.get_fixtures_json(header_url, fixtures_url, header_api_key, season, league_id, fixtures_next_num, requests, sys, json, datetime)
    f_df = f.fixtures_transformation(f_json_data, pd)
    f_ans = f.fixtures_to_staging(to_staging_string, f_df, fixtures_df_type, create_engine)

#data injestion, transformation and load to staging tables of match results
    r_json_data = r.get_results_json(header_url, results_url, header_api_key, season, league_id, requests, sys, json, datetime)
    r_df = r.results_transformation(r_json_data, pd)
    r_ans = r.results_to_staging(to_staging_string, r_df, results_df_type, create_engine)

    


#if all the staging functions are successfull they will return True
#data is moved from staging tables to core tables
#If error in moving data to core and susccful core loads are rolled back.
#staging tables are then truncated
    if ta_ans == True and ts_ans == True and s_ans == True and f_ans == True and r_ans == True:
        try:
            staging_to_core(core_host, core_user, core_password, core_dbname, core_port)
        except:
            print('staging_to_core function failed to complete')
        finally:
            truncate_staging_tables(core_host, core_user, core_password, core_dbname, core_port)
    else:
        print('''Error: all to_staging functions are required to be completed before staging_to core\n
                        #can be executed''')
        
        print(print(f'{top_assisters_df_type} export to database staging area failed.'))
