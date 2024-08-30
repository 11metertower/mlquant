import os
import pandas as pd
import pymysql
from dotenv import load_dotenv
from tqdm import tqdm

def insert_yahoo(folderpath, stuff):
    """크롤링한 주가 데이터를 DB에 insert하는 함수
    
    Args:
        folderpath: table에 넣을 csv파일들이 있는 경로
        stuff: 데이터들을 insert할 table 이름이 들어갈 변수
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

    query = f"""
        INSERT INTO {stuff} (BASE_DATE,OPEN_PRICE,HIGH_PRICE,LOW_PRICE,CLOSE_PRICE,ADJ_CLOSE_PRICE,VOLUME,ITEM_CODE,ITEM_NAME,CCY,MARKET_DIV)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """

    for (root, _, files) in os.walk(folderpath):
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

    con.commit()
    con.close()

if __name__ == "__main__":
    print("######   Insert CSV files to DB   ######")
    folderpath = input("Input file path: ")
    print("Select DB name\n1. YAHOO\n2. YAHOO_KOSPI_200")
    menu = int(input())
    if menu == 1:
        menu = "YAHOO"
    elif menu == 2:
        menu = "YAHOO_KOSPI_200"
    else:
        print("Wrong input!!")
        exit()
    insert_yahoo(folderpath, menu)