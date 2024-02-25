def get_results_json(url, r_url, api_key, season, leagueid, requests, sys, json, datetime):
    fromm = datetime.date.today() - datetime.timedelta(days=14)
    to = datetime.date.today()
    header_param = {'X-RapidAPI-Key': api_key,
                'X-RapidAPI-Host': url}

    # dictionary for parameters that specify data i want
    query_param = {'league': leagueid, 'season': season, 'from':fromm, 'to':to}

    try:
        res = requests.get(r_url, params=query_param, headers=header_param)
    except Exception as e:
        print(f'Request to url failed\n{str(e)}')
        sys.exit()

    if res.status_code == 200:   
        json_data = json.loads(res.text)
        return json_data
    else:
        print('Couldnt get data.')
        print(res.status_code)

def results_transformation(json_data, pd):
    data = json_data['response'][0:]
    df = pd.json_normalize(data)
    df = pd.DataFrame(df)
    
        
    #remove nulls (games that aint been played yet but fixtured for today)
    df = df[df['score.fulltime.away'].notnull()]

    #create date and time columns
    df['fixture.timestamp'] = pd.to_datetime(df['fixture.timestamp'],unit='s')
    df['kickoff_date'] = df['fixture.timestamp'].dt.date
    df['kickoff_time'] = df['fixture.timestamp'].dt.time

    #change goals to int
    df['goals.home'] = df['goals.home'].astype('int')
    df['goals.away'] = df['goals.away'].astype('int')
    df['score.halftime.home'] = df['score.halftime.home'].astype('int')
    df['score.halftime.away'] = df['score.halftime.away'].astype('int')
    df['score.fulltime.home'] = df['score.fulltime.home'].astype('int')
    df['score.fulltime.away'] = df['score.fulltime.away'].astype('int')

    #drop datetime column

    #order by date & time ascending
    df = df.sort_values(['kickoff_date','kickoff_time'],ascending = [False, False])

                      
    df = df[['fixture.id','kickoff_date','kickoff_time','teams.home.name','teams.away.name','goals.home','goals.away',
        'score.halftime.home','score.halftime.away','score.fulltime.home','score.fulltime.away']]
    return df


def results_to_staging(string, df, df_type, create_engine):
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