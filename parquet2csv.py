import click
import pandas as pd
import sys
import os
# df = pd.read_parquet('/Users/yhua/Desktop/parquet/userdata1.parquet')

@click.command()
@click.option('--path', default = "", help="path to the dir of parquets")
# @click.option('--maxLine', help = "max lines in one csv output")
def readFiles(path):
    '''
    read all files in path and save parquet file as csv
    :param path: path of the directory of parquets
    :return: None
    '''
    path = str(path)
    if not path:
        print("directory path to parquet needed!")
        return
    counter = 0
    for file in os.listdir(path):
        if file.endswith(".parquet"):
            counter+=1
            df = pd.read_parquet(path + file)
            saveFile(df, path + '/' + str(counter) + '.csv')

    return

def saveFile(df, path):
    '''
    save the dataframe to csv. Overwrite the file if name already exists.
    :param df: dataframe to write as csv
    :param path: path to csv file
    :return: None
    '''
    df.to_csv(path)

if __name__ == '__main__':

    readFiles()