
def get_standings_json(url, s_url, api_key, season, leagueid, requests, sys, json):
    # dictionary for header parameters
    header_param = {'X-RapidAPI-Key': api_key,
                    'X-RapidAPI-Host': url}

    # dictionary for parameters that specify data i want
    query_param = {'league': leagueid, 'season': season}

    try:
        res = requests.get(s_url, params=query_param, headers=header_param)
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



def standings_transformation(json_data, pd):
    data = json_data['response'][0]['league']['standings'][0]
    df = pd.json_normalize(data)

    df = pd.DataFrame(df)

    df['GD'] = df['all.goals.for'] - df['all.goals.against']
        #'all.goals.for': 'GF'
    df = df.rename({'team.name': 'club', 'all.played': 'MP', 'all.win': 'W', 'all.draw': 'D', 'all.lose': 'L','all.goals.for': 'GF'
                   , 'all.goals.against': 'GA', 'points': 'PTS', 'form': 'last 5', 'all.goals.for': 'GF'}, axis=1)

    df = df[['rank', 'club', 'MP', 'W', 'D', 'L', 'GF','GA','GD','PTS', 'last 5']]
        
    return df


def standings_to_staging(string, df, df_type, create_engine):
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