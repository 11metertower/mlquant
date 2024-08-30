import pandas as pd
import pymysql
from dotenv import load_dotenv
import os
from tqdm import tqdm
import argparse

def main(input, output, start_date, end_date):
    """KRX_ADJUST 테이블에서 조정된 주가인 ADJUST_PRICE와 YAHOO_KOSPI_200 테이블에서 조정된 주가인 CLOSE_PRICE
    사이의 상관관게와 차이의 퍼센트 합을 계산하는 함수

    Args:
        input: 주가를 비교할 종목들이 들어있는 input file의 이름을 .csv를 붙이지 않고 이름만 str으로 받는다.
        output: output file의 이름을 .csv를 붙이지 않고 이름만 str으로 받는다.
        start_date: 어느 날짜부터 주가를 비교할지 yyyy-mm-dd의 형식으로 str으로 받는다.
        end_date: 어느 날짜까지 주가를 비교할지 yyyy-mm-dd의 형식으로 str으로 받는다.
    """
    kospi_list = pd.read_csv(f"{input}.csv", dtype=object)
    kospi_list = kospi_list['종목코드'].to_list()
    kospi_list_2 = str(kospi_list).split('[')[1].split(']')[0]

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

    # 쿼리로 KRX_ADJUST 테이블의 값과 YAHOO_KOSPI_200 테이블의 값을 left join하여 받는다.
    query = f"""SELECT a.BASE_DATE, a.ITEM_CODE, a.ADJUST_PRICE, b.CLOSE_PRICE FROM KRX_ADJUST AS a LEFT JOIN YAHOO_KOSPI_200 AS b 
            ON a.BASE_DATE = b.BASE_DATE AND a.ITEM_CODE = b.ITEM_CODE WHERE a.ITEM_CODE IN ({kospi_list_2}) 
            AND a.BASE_DATE >= "{start_date}" AND a.BASE_DATE <= "{end_date}" ORDER BY a.ITEM_CODE, a.BASE_DATE;"""

    mycursor.execute(query)

    df = mycursor.fetchall()
    df = pd.DataFrame(df)

    result = pd.DataFrame()

    length = len(df)
    idx_start = 0
    try:
        current_item = df.loc[0, 'ITEM_CODE']
    except:
        print("Doesn't have any log!")
        exit()
    idx = 0  # result dataframe 에 저장할 때 필요한 index
    sum = 0  # percentage difference 의 합을 저장할 변수
    cnt_a = 0  # KRX_ADJUST 에 몇 일의 데이터가 있는지 저장할 변수
    cnt_y = 0  # YAHOO_KOSPI_200 에 몇 일의 데이터가 있는지 저장할 변수
    cnt = 0  # 공통으로 몇 일의 데이타가 있는지 저장할 변수
    for i in tqdm(range(length)):
        if current_item != df.loc[i, 'ITEM_CODE']:  # 다음 주식으로 넘어가면 넘어가기 전의 주식의 값들을 result dataframe에 모두 저장한다.
            result.loc[idx, 'ITEM_CODE'] = current_item
            result.loc[idx, 'KRX_DATES_COUNT'] = int(cnt_a)
            result.loc[idx, 'YAHOO_DATES_COUNT'] = int(cnt_y)
            result.loc[idx, 'COMMON_DATES_COUNT'] = int(cnt)
            result.loc[idx, 'COMMON_DATES_ADJUST_CORR'] = df.iloc[idx_start:i].loc[:,['ADJUST_PRICE', 'CLOSE_PRICE']].corr(method='pearson', min_periods=1).loc['ADJUST_PRICE', 'CLOSE_PRICE']
            if pd.isna(result.loc[idx, 'COMMON_DATES_ADJUST_CORR']):
                result.loc[idx, 'COMMON_DATES_ADJUST_CORR'] = 0.0
            result.loc[idx, 'COMMON_DATES_ADJUST_PERCENT_DIFF_SUM'] = sum
            sum = 0
            current_item = df.loc[i, 'ITEM_CODE']
            idx_start = i
            idx += 1
            cnt_a = 0
            cnt_y = 0
            cnt = 0
        
        if not pd.isna(df.loc[i, 'ADJUST_PRICE']):
            cnt_a += 1
        if not pd.isna(df.loc[i, 'CLOSE_PRICE']):
            cnt_y += 1
        if not pd.isna(df.loc[i, 'ADJUST_PRICE']) and not pd.isna(df.loc[i, 'CLOSE_PRICE']):  # ADJUST_PRICE 와 CLOSE_PRICE 둘 다 값이 있으면 percentage difference를 계산한다.
            cnt += 1
            df.loc[i, 'COMMON_DATES_ADJUST_PERCENT_DIFF'] = abs(float(df.loc[i, 'ADJUST_PRICE']) - float(df.loc[i, 'CLOSE_PRICE'])) / float(df.loc[i, 'CLOSE_PRICE'])
            sum += df.loc[i, 'COMMON_DATES_ADJUST_PERCENT_DIFF']
    
    # 마지막 종목에 대한 값들을 result 에 저장한다.
    result.loc[idx, 'ITEM_CODE'] = current_item
    result.loc[idx, 'KRX_DATES_COUNT'] = int(cnt_a)
    result.loc[idx, 'YAHOO_DATES_COUNT'] = int(cnt_y)
    result.loc[idx, 'COMMON_DATES_COUNT'] = int(cnt)
    result.loc[idx, 'COMMON_DATES_ADJUST_CORR'] = df.iloc[idx_start:i+1].loc[:,['ADJUST_PRICE', 'CLOSE_PRICE']].corr(method='pearson', min_periods=1).loc['ADJUST_PRICE', 'CLOSE_PRICE']
    if pd.isna(result.loc[idx, 'COMMON_DATES_ADJUST_CORR']):
        result.loc[idx, 'COMMON_DATES_ADJUST_CORR'] = 0.0
    result.loc[idx, 'COMMON_DATES_ADJUST_PERCENT_DIFF_SUM'] = sum

    result.to_csv(f"{output}.csv", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-input", type=str, dest="input", action="store", default="KOSPI200list")
    parser.add_argument("-output", type=str, dest="output", action="store", default="result")
    parser.add_argument("-start_date", type=str, dest="start_date", action="store", default="2001-01-02")
    parser.add_argument("-end_date", type=str, dest="end_date", action="store", default="2024-07-30")
    args = parser.parse_args()

    main(args.input, args.output, args.start_date, args.end_date)