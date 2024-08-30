import os
import pandas as pd
import pymysql
from dotenv import load_dotenv
import numpy as np

def insert_krx(menu, path):
    """DB에 insert하는 함수
    
    Args:
        menu: 어떤 table에 insert할지 정하는 변수
                1. KRX_OHLCV, 2. KRX_INDUSTRY_CLASSIFICATION, 3. KRX_INDEX_COMPOSITION 4. KRX_FOREIGNER_HOLD 5. KRX_PER_PBR_DIV 6. KRX_STOCK_SUBSTITUTE 7. KRX_INVESTOR_BUY_TOP  8. KRX_IDX_PRICE 9. KRX_IDX_PER_PBR_DIV
        path: table에 넣을 csv파일이 있는 경로
    """
    load_dotenv()
    try:
        con = pymysql.connect(user=os.environ.get('DBid'),
                            passwd=os.environ.get('DBpasswd'),
                            host=os.environ.get('DBipaddr'),
                            port=13306,
                            db=os.environ.get('DBid'),
                            charset='utf8')
    except:
        con = pymysql.connect(user=os.environ.get('DBid'),
                            passwd=os.environ.get('DBpasswd'),
                            host=os.environ.get('DBhost'),
                            port=3306,
                            db=os.environ.get('DBid'),
                            charset='utf8')
    
    mycursor = con.cursor()
    if menu == 1:
        query = f"""
            INSERT INTO KRX_OHLCV (ITEM_CODE,ITEM_NAME,MARKET_DIV,DEPT,CLOSE_PRICE,DIFF,PCT_CHG,OPEN_PRICE,HIGH_PRICE,LOW_PRICE,TRADE_VOL,TRANS_AMT,MKT_CAP,LISTED_SHARES_NUM,BASE_DATE)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """
    elif menu == 2:
        query = f"""
            INSERT INTO KRX_INDUSTRY_CLASSIFICATION (ITEM_CODE,ITEM_NAME,MARKET_DIV,INDUSTRY_NAME,CLOSE_PRICE,DIFF,PCT_CHG,MKT_CAP,BASE_DATE)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """
    elif menu == 3:
        query = f"""
            INSERT INTO KRX_INDEX_COMPOSITION (ITEM_CODE,ITEM_NAME,CLOSE_PRICE,DIFF,PCT_CHG,MKT_CAP,BASE_DATE,MARKET_DIV)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
            """
    elif menu == 4:
        query = f"""
            INSERT INTO KRX_FOREIGNER_HOLD (BASE_DATE,ITEM_CODE,ITEM_NAME,CLOSE_PRICE,DIFF,PCT_CHG,LISTED_SHARES,FOREIGNER_OWN,FOREIGNER_RATIO,FOREIGNER_LIMIT,FOREIGNER_EXHAUST)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """
    elif menu == 5:
        query = f"""
            INSERT INTO KRX_PER_PBR_DIV (BASE_DATE,ITEM_CODE,ITEM_NAME,CLOSE_PRICE,DIFF,PCT_CHG,EPS,PER,PRE_EPS,PRE_PER,BPS,PBR,DIV_PER_SHARE,DIV_YIELD)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """
    elif menu == 6:
        query = f"""
            INSERT INTO KRX_STOCK_SUBSTITUTE (BASE_DATE,ITEM_CODE,ITEM_NAME,SUBSTITUTE,PROD_DATE,CLOSE_PROD)
            VALUES (%s,%s,%s,%s,%s,%s);
            """
    elif menu == 7:
        query = f"""
            INSERT INTO KRX_INVESTOR_BUY_TOP (BASE_DATE,ITEM_CODE,ITEM_NAME,SELL_AMT,BUY_AMT,NETBUY_AMT,SELL_KRW,BUY_KRW,NETBUY_KRW,INVESTOR)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """
    elif menu == 8:
        query = f"""
            INSERT INTO KRX_IDX_PRICE (BASE_DATE,MARKET_DIV,IDX_NAME,CLOSE_PRICE,DIFF,PCT_CHG,OPEN_PRICE,HIGH_PRICE,LOW_PRICE,TRADE_VOL,TRANS_AMT,MKT_CAP)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """
    elif menu == 9:
        query = f"""
            INSERT INTO KRX_IDX_PER_PBR_DIV (BASE_DATE,MARKET_DIV,IDX_NAME,CLOSE_PRICE,DIFF,PCT_CHG,PER,PBR,DIV_YIELD)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """
    df = pd.read_csv(path, low_memory=False)
    print("Loading csv file is complete!")

    if menu >= 1 and menu <= 7:
        df['ITEM_CODE'] = df['ITEM_CODE'].apply(lambda x : str(x).zfill(6))
        print("Modifying ITEM_CODE is complete!")

    if menu == 1:
        df.fillna({'DEPT': '소속부없음'}, inplace=True)
    
    df = df.replace({np.nan: None})
    print("Replacing NaN is complete!")
    
    args = df.values.tolist()

    try:
        mycursor.executemany(query, args)
    except Exception as e:
        print(e)
        con.close()
        exit()
        
    con.commit()

    con.close()

if __name__ == "__main__":
    print("######   Insert CSV files to DB   ######")
    print("Which data do you want to insert?\n1. KRX_OHLCV\n2. KRX_INDUSTRY_CLASSIFICATION\n3. KRX_INDEX_COMPOSITION\n4. KRX_FOREIGNER_HOLD\n5. KRX_PER_PBR_DIV\n6. KRX_STOCK_SUBSTITUTE\n7. KRX_INVESTOR_BUY_TOP\n8. KRX_IDX_PRICE\n9. KRX_IDX_PER_PBR_DIV")
    menu = int(input())
    if menu >= 1 and menu <= 9:
        path = input("Input file path: ")
    else:
        print("Wrong number!!")
        exit()
    insert_krx(menu, path)