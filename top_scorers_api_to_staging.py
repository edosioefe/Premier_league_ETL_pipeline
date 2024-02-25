import sys
def get_top_scorers_json(url, ts_url, api_key, season, leagueid, requests, sys, json):
    
    header_param = {'X-RapidAPI-Key': api_key,
                'X-RapidAPI-Host': url}

    # dictionary for parameters that specify data i want
    query_param = {'league': leagueid, 'season': season}

    try:
        res = requests.get(ts_url, params=query_param, headers=header_param)
    except Exception as e:
        print(f'Request to url failed\n{str(e)}')
        sys.exit()

    if res.status_code == 200:   
        json_data = json.loads(res.text)
        return json_data
    else:
        print('Couldnt get data.')
        print(res.status_code)



# transform data
def top_scorers_transformation(json_data, pd):
        data = json_data['response'][0:][0]
        df = pd.json_normalize(data)
        
        #nested dictionary of stats
        stat_df = pd.json_normalize(data['statistics'][0])
        #joining the two datasets
        df = pd.concat([df[['player.name','player.firstname','player.lastname']],
                    stat_df[['team.name','games.appearences','goals.total']] ],axis=1)
        df = df.head()
        return df



def top_scorers_to_staging(string, df, df_type, create_engine):
    try:
        engine = create_engine(string)
        #ADD SOME TO CONFIGURATION FILE!!!!!!!!!!!!
        df.to_sql(df_type, engine, if_exists='replace', schema='staging', index=False)
        print(f'{df_type} Dataframe has been exported to database staging area.')
        return True
    except Exception as e:
        print(f'{df_type} export to database staging area failed.')
        print(str(e)) 
        return False
    finally:
        engine.dispose()
