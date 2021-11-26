import pandas as pd
from sqlalchemy import create_engine
from configparser import ConfigParser
import requests
import time
import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler

configure = ConfigParser()
configure.read('config.ini')

mysqlUsername = configure.get('mysql', 'username')
mysqlPassword = configure.get('mysql', 'password')
mysqlHost = configure.get('mysql', 'host')
mysqlDb = configure.get('mysql', 'db')
filename = configure.get('file', 'filename')
fileUrl = configure.get('file', 'fileurl')


def log_setup():
    log_handler = TimedRotatingFileHandler("app.log",
                                           when="d",
                                           interval=1,
                                           backupCount=5)
    formatter = logging.Formatter(
        '%(asctime)s : %(levelname)s - process [%(process)d]: %(message)s',
        '%b %d %H:%M:%S')
    formatter.converter = time.gmtime  # if you want UTC time
    log_handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG)


def downloadFile():
    r = requests.get(fileUrl)
    with open(filename, 'wb') as f:
        f.write(r.content)
        f.close()


def getList(dict):
    return list(dict.keys())


def getDbConnection():
    return create_engine("mysql://{username}:{password}@{host}/{db}".format(
        username=mysqlUsername, password=mysqlPassword,
        host=mysqlHost, db=mysqlDb), echo=True)


# adding data frames into a list
def getDataFramesAsList(sheetNames, dfs):
    totalDf = []
    for sheet in sheetNames:
        totalDf.append(dfs[sheet])
    return totalDf


log_setup()
downloadFile()
dfs = pd.read_excel(filename, sheet_name=None)
sheetNames = getList(dfs)


# merging multiple dataframes into one based on how many sheets file contains
totalDf = pd.concat(getDataFramesAsList(sheetNames, dfs))
totalDf.reset_index().set_index('IFSC')
# inserting into table following with same coloumn name from sheet
totalDf.to_sql(con=getDbConnection(), name='bank_ifsc_temp',
               if_exists='replace', index=False)
