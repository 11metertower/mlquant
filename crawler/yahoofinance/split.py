import os
import pandas as pd
from tqdm import tqdm
import shutil
import yfinance as yf
import pymysql
from dotenv import load_dotenv

def crawl(folder_name):
    """액면분할 데이터를 크롤링하는 함수

    Args:
        folder_name (str): 데이터들이 저장될 폴더의 이름을 지정하는 변수
    """
    if not os.path.exists(f'./splits/{folder_name}'):
        os.makedirs(f'./splits/{folder_name}')
    table = pd.read_csv(f"{folder_name}list.csv", dtype=object)
    table = table.values.tolist()
    
    for element in tqdm(table):
        ticker = element[0]
        name = element[1]
        if folder_name == "KOSPI200":
            real_ticker = ticker + ".KS"
        elif folder_name == "KOSDAQ150":
            real_ticker = ticker + ".KQ"
        else:
            real_ticker = ticker
        
        info = yf.Ticker(real_ticker)
        df = info.splits

        if not df.empty:
            df = pd.DataFrame({'BASE_DATE':df.index.strftime('%Y-%m-%d'), 'STOCK_SPLITS':df.values})
            df['ITEM_CODE'] = ticker
            df['ITEM_NAME'] = name

            if folder_name == "KOSPI200":
                df['MARKET_DIV'] = "KOSPI 200"
            elif folder_name == "KOSDAQ150":
                df['MARKET_DIV'] = "KOSDAQ 150"
            elif folder_name == "NASDAQ100":
                df['MARKET_DIV'] = "NASDAQ 100"
            else:
                df['MARKET_DIV'] = "S&P 500"

            df.to_csv(f'./splits/{folder_name}/{ticker}.csv', index=False)

def insert(folder_name):
    """액면분할 데이터를 DB에 넣는 함수

    Args:
        folder_name (str): 데이터들이 저장되어 있는 폴더의 이름을 지정하는 변수
    """
    folder_name = "./splits/" + folder_name
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

    query = f"""
        INSERT INTO YAHOO_SPLITS (BASE_DATE,STOCK_SPLITS,ITEM_CODE,ITEM_NAME,MARKET_DIV)
        VALUES (%s,%s,%s,%s,%s);
        """
    
    for (root, _, files) in os.walk(folder_name):
        for file in tqdm(files):
            file_path = os.path.join(root, file)
            df = pd.read_csv(file_path, low_memory=False)
            if (df['MARKET_DIV'] == 'KOSDAQ 150').any() or (df['MARKET_DIV'] == 'KOSPI 200').any():
                df['ITEM_CODE'] = df['ITEM_CODE'].apply(lambda x : str(x).zfill(6))
            args = df.values.tolist()

            try:
                mycursor.executemany(query, args)
            except Exception as e:
                print(e)
                exit()
    
    con.commit()
    con.close()


if __name__ == "__main__":
    menu = int(input("1. crawl splits\n2. insert in DB\n"))
    if menu == 1:
        crawl("KOSDAQ150")
        crawl("KOSPI200")
        crawl("NASDAQ100")
        crawl("S&P500")
    elif menu == 2:
        insert("KOSDAQ150")
        insert("KOSPI200")
        insert("NASDAQ100")
        insert("S&P500")
    else:
        print("Wrong input!!")