import subprocess
import requests
import os
import pandas as pd
import datetime
from datetime import datetime

def download_file():
    try:
        url = 'https://github.com/LuisGustavo2010/Datapipeline_Test_Raizen/raw/main/raw_data/vendas-combustiveis-m3.xls'
        filename = 'vendas-combustiveis-m3.xls'

        response = requests.get(url, stream=True)

        with open(os.path.join("/home/luisdev/Documentos/Projetos/Datapipeline_raizen/data/raw_data", filename), "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
    except Exception as e:
        print(f'Ocorreu um erro: {e}')
        
    try:
        subprocess.run(['libreoffice', '--headless', '--convert-to', 'ods', '--outdir', '/home/luisdev/Documentos/Projetos/Datapipeline_raizen/data/raw_data', 'vendas-combustiveis-m3.xls'])
    except:
        pass

def Pandas_transform():
    #Concatenando dfs
    try:
      file = '/home/luisdev/Documentos/Projetos/Datapipeline_raizen/data/raw_data/vendas-combustiveis-m3.xls'
      aba1_df = pd.read_excel(file, sheet_name=1)
      aba2_df = pd.read_excel(file, sheet_name=2)
      aba3_df = pd.read_excel(file, sheet_name=3)
      dfs = [aba1_df, aba2_df, aba3_df] 
      combined_df = pd.concat(dfs)
      df = combined_df
    except Exception as e:
      print(f'Ocorreu um erro: {e}')

    #tratamentos
    try: 
      path='/home/luisdev/Documentos/Projetos/Datapipeline_raizen/data/silver/cleanData.csv'
      df.columns = ['Combustível', 'Ano', 'Região', 'UF', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', 'Total']
      df = df.melt(id_vars=['Combustível', 'Ano', 'Região', 'UF'])
      df = df.loc[df['variable'] != 'Total']
      df['year_month'] = df['Ano'].astype(str) + '-' + df['variable']
      df['year_month'] = pd.to_datetime(df['year_month'])
      df = df.drop(labels=['variable', 'Região', 'Ano'], axis=1)
      df.columns = ['product', 'uf', 'volume', 'year_month']
      df['volume'] = pd.to_numeric(df['volume'])
      df['product'] = df['product'].str.replace(' \(m3\)', '', regex=True)
      df = df.fillna(0)
      df['unit'] = 'm3'
      df['created_at'] = datetime.now()
      df = df[['year_month', 'uf', 'product', 'unit', 'volume', 'created_at']]
      df.to_csv(path, index=False, sep=';')
    except Exception as e:
      print(f'Ocorreu um erro: {e}')

def VendasDerivadosPetroleo():
    path = 'data/gold/VendasDerivadosPetroleo.csv'
    df = pd.read_csv('data/silver/cleanData.csv', sep=';')
    df = df[~df['product'].str.startswith('ÓLEO DIESEL (OUTROS )')] 
    df = df[~df['product'].str.startswith('ÓLEO DIESEL MARÍTIMO')] 
    df = df[~df['product'].str.startswith('ÓLEO DIESEL S-10')] 
    df = df[~df['product'].str.startswith('ÓLEO DIESEL S-1800')]  
    df = df[~df['product'].str.startswith('ÓLEO DIESEL S-500')] 
    df = df[~df['product'].str.startswith('GLP - Até P13')] 
    df = df[~df['product'].str.startswith('GLP - Outros')] 
    df.to_csv(path, index=False, sep=';')

def VendasDerivadosPetroleo():
    path='data/gold/VendasDiesel.csv'
    df = pd.read_csv('data/silver/cleanData.csv', sep=';')
    df = df[df['product'] != 'ÓLEO DIESEL']
    df = df[df['product'].str.startswith('ÓLEO DIESEL')]
    df.to_csv(path, index=False, sep=';')