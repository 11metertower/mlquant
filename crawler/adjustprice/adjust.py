import pandas as pd
import os
import pymysql
import datetime
from dotenv import load_dotenv
import argparse
from tqdm import tqdm
import numpy as np

def calc_adjust_price(item_list, start_date, output_file_name):
    """item_list에 있는 종목들을 start_date부터 현재까지 액면분할 이력을 참조하여 현재가 기준으로 조정하여
    {output_file_name}.csv로 저장하는 함수이다.
    SPLIT table에서 해당 종목들의 start_date부터의 주가 이력들과 액면분할 이력을 join하여 가져오고,
    가져온 데이터를 바탕으로 역산하여 주가를 현재가로 조정한다.

    Args:
        item_list: 주가를 조정할 종목들의 item code를 list로 받는다.
        start_date: 어느 날짜부터 주가를 조정할지 yyyy-mm-dd의 형식으로 str으로 받는다.
        output_file_name: output file의 이름을 .csv를 붙이지 않고 이름만 str으로 받는다.
    """
    load_dotenv()
    try:
        con = pymysql.connect(user=os.environ.get('DBid'),
                            passwd=os.environ.get('DBpasswd'),
                            host=os.environ.get('DBipaddr'),
                            port=13306,
                            db=os.environ.get('DBid'),
                            charset='utf8',
                            cursorclass=pymysql.cursors.DictCursor)
    except:
        con = pymysql.connect(user=os.environ.get('DBid'),
                            passwd=os.environ.get('DBpasswd'),
                            host=os.environ.get('DBhost'),
                            port=3306,
                            db=os.environ.get('DBid'),
                            charset='utf8',
                            cursorclass=pymysql.cursors.DictCursor)
        
    mycursor = con.cursor()
    query = f"""SELECT a.BASE_DATE, a.ITEM_CODE, a.CLOSE_PRICE, a.TRANS_AMT, a.MKT_CAP, b.STOCK_SPLITS FROM KRX_OHLCV a LEFT JOIN SPLITS b ON a.ITEM_CODE=b.ITEM_CODE and a.BASE_DATE=b.BASE_DATE
        WHERE a.item_code in ({item_list})
        AND a.BASE_DATE >= '{start_date}'
        order by a.ITEM_CODE, a.BASE_DATE;"""
    
    mycursor.execute(query)

    result = mycursor.fetchall()
    result = pd.DataFrame(result)

    ratio = 1  # 주가가 조정될 비율이 저장되는 변수
    code = result.loc[len(result)-1, 'ITEM_CODE']  # 현재 조정하고 있는 종목의 코드
    date = result.loc[len(result)-1, 'BASE_DATE']  # 중복된 데이터를 판별하기 위한 변수
    
    for i in tqdm(range(len(result)-1, -1, -1)):  # Dataframe의 가장 아래부터 보면서 주가를 조정한다.
        if code != result.loc[i, 'ITEM_CODE']:  # 조정하고 있는 종목이 바뀌면 비율을 1로 다시 바꾸고, 코드도 변경한다.
            ratio = 1
            code = result.loc[i, 'ITEM_CODE']
        elif result.loc[i, 'BASE_DATE'] == date:  # 중복된 데이터가 존재하면 해당 행 삭제
            result.drop([i], axis=0, inplace=True)
            continue
        date = result.loc[i, 'BASE_DATE']
        result.loc[i, 'ADJUST_PRICE'] = float(result.loc[i, 'CLOSE_PRICE']) / ratio  # 주가에 ratio를 나눠서 주가를 조정한다.
        if(not pd.isna(result.loc[i, 'STOCK_SPLITS'])):  # 액면분할 이력이 있는 날짜이면 ratio에 비율을 곱한다.
            ratio *= float(result.loc[i, 'STOCK_SPLITS'])

    result.drop(['STOCK_SPLITS'], axis=1, inplace=True)

    result.to_csv(f"{output_file_name}.csv", index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-input", type=str, dest="input", action="store")
    parser.add_argument("-output", type=str, dest="output", action="store", default="output")
    parser.add_argument("-start_date", type=str, dest="start_date", action="store", default="2000-01-01")
    args = parser.parse_args()

    if args.input == None or args.input == "":
        input = pd.read_csv("KRXlist.csv", dtype=object)
    else:
        input = pd.read_csv(f"{args.input}.csv", dtype=object)
    
    input = input["ITEM_CODE"].to_list()
    input = str(input).split('[')[1].split(']')[0]

    calc_adjust_price(input, args.start_date, args.output)