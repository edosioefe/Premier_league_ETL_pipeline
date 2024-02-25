class ExportDf():

    def __init__(self, df, df_type, date):

        self.df = df
        self.df_type = df_type
        self.date = date

    def df_to_folder(self):
        
        self.df.to_csv('C://Users//Efe//football_project//pythonProject1//exported_csv_files//' +
                        self.df_type + '_' + self.date + '.csv', index=False)
            

