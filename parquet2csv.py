import pandas as pd
import sys
import os
# df = pd.read_parquet('/Users/yhua/Desktop/parquet/userdata1.parquet')


def readFiles(path, max_line):
    '''
    read all files in path and save parquet file as csv
    :param path: path of the directory of parquets
    :return: None
    '''
    max_line = int(max_line)
    path = str(path)
    if not path:
        print("directory path to parquet needed!")
        return
    counter = 0
    df_left = pd.DataFrame()
    leftLineNb = 0
    for file in os.listdir(path):
        if file.endswith(".parquet"):

            df = pd.read_parquet(path + file)
            if(max_line > len(df)):
                print("max_line bigger than number of lines in the file!")
                return
            df = pd.concat([df_left,df])
            times = (leftLineNb + len(df)) // max_line
            slices = [[i * max_line, (i+1) * max_line-1] for i in range(times)]
            for i in slices:
                df2write = df[i[0] : i[1]+1]
                print(len(df2write))
                counter += 1
                saveFile(df2write, path + '/' + str(counter) + '.csv')
            df_left = df[times * max_line : -1]
            leftLineNb = len(df_left)
    if not df_left.empty:
        counter+=1
        saveFile(df_left, path + '/' + str(counter) + '.csv')
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
    readFiles(sys.argv[1],sys.argv[2])