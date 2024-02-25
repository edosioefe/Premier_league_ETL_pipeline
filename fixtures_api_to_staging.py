def get_fixtures_json(url, f_url, api_key, season, leagueid, next_num_f, requests, sys, json, datetime):
    #fromm = datetime.date.today() 
    #to = datetime.date.today() + datetime.timedelta(days=7)
    # dictionary for header parameters
    header_param = {'X-RapidAPI-Key': api_key,
                    'X-RapidAPI-Host': url}

    # dictionary for parameters that specify data i want
    query_param = {'league': leagueid, 'season': season, 'next': next_num_f}

    try:
        res = requests.get(f_url, params=query_param, headers=header_param)
    except Exception as e:
        print(f'Request to url failed\n{str(e)}')
        sys.exit()

    # check if status code is 200 (OK)
    if res.status_code == 200:
        # data transformation
        json_data = json.loads(res.text)
        return json_data
    else:
        print('Couldnt get data.')
        print(res.status_code)


def fixtures_transformation(json_data, pd):
    data = json_data['response'][0:]
    df = pd.json_normalize(data)
    df = pd.DataFrame(df)
    
    #get columns i need
    df = df[['fixture.id','fixture.timestamp','teams.home.name', 'teams.away.name']]

#create date and time columns
    df['fixture.timestamp'] = pd.to_datetime(df['fixture.timestamp'],unit='s')
    df['kickoff_date'] = df['fixture.timestamp'].dt.date
    df['kickoff_time'] = df['fixture.timestamp'].dt.time


#order by date & time ascending
    df = df.sort_values(['kickoff_date','kickoff_time'],ascending = [True, True])

#final columns i need                    
    df = df[['fixture.id','kickoff_date','kickoff_time','teams.home.name','teams.away.name']]
    return df
def fixtures_to_staging(string, df, df_type, create_engine):
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