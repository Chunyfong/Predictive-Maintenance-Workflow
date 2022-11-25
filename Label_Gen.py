import pandas as pd
import pymssql
import datetime
import numpy as np
import luigi
from sqlalchemy import create_engine
import datetime
import os
import shutil
import gc
import urllib
from sqlalchemy import event
from sqlalchemy import create_engine

# params = {'latest': datetime.datetime.today() - datetime.timedelta(days=100)}

# Get data from SQL Server

params = urllib.parse.quote_plus(
    "Driver={ODBC Driver 17 for SQL Server};Server=" ";DATABASE=Master;UID="ID";PWD="PW";")
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
conn = engine.connect()

class rawA(luigi.Task):

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget(r"raw_A.csv")

    def run(self):
        df = pd.read_sql("""SELECT [ID],[Date],[Para_1],.......
                            FROM .[dbo].[raw_A]
                            WHERE DATEDIFF(day,[Date],GETDATE()) between 0 and 365 """, con=conn)
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d %h:%m:%s').dt.date
        df = df.fillna(0)
        df.iloc[:, 2:] = df.iloc[:, 2:].astype("int32")
        df = df.groupby(['ID', 'Date'], as_index=False).max()
        df = df.drop_duplicates()
        # You can backwards or forwards fill all the date inbetween the complete the time series
        df = (df.set_index('Date').groupby('ID').apply(
            lambda d: d.reindex(pd.date_range(min(d.index), max(d.index), freq='D')))
              .drop('MACH_ID', axis=1).reset_index('MACH_ID').ffill().bfill().reset_index())
        df.iloc[:, 2:] = df.iloc[:, 2:].astype("int32")
        df.to_csv(r'Raw_A', index=False)     
        
