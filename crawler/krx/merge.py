import os
import pandas as pd
from tqdm import tqdm

def merge(menu, paths, filename):
    """크롤링한 csv파일들을 DB에 넣기 쉽도록 하나의 csv파일로 합치는 함수

    Args:
        menu: 어떤 table에 넣을 데이터를 합칠지 정하는 변수
                1. KRX_OHLCV, 2. KRX_INDUSTRY_CLASSIFICATION, 3. KRX_INDEX_COMPOSITION 4. KRX_FOREIGNER_HOLD 5. KRX_PER_PBR_DIV 6. KRX_STOCK_SUBSTITUTE 7. KRX_INVESTOR_BUY_TOP 8. KRX_IDX_PRICE 9. KRX_IDX_PER_PBR_DIV
        paths: 어느 경로에 합칠 csv파일들이 있는지 경로들의 list
        filename: 합친 csv파일의 이름
    """
    new_df = pd.DataFrame()
    for i, folderpath in enumerate(paths):
        con_df = pd.DataFrame()
        for (root, _, files) in os.walk(folderpath):
            for file in tqdm(files):
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path)
                if menu != 6:
                    df['BASE_DATE'] = file.split('.')[0]
                else:
                    df['적용일'] = df['적용일'].str.replace('-', '')
                    df['산출기준일'] = df['산출기준일'].str.replace('/', '')

                if menu == 1:
                    df = df.rename(columns={'종목코드': 'ITEM_CODE', '종목명': 'ITEM_NAME', '시장구분': 'MARKET_DIV', '소속부': 'DEPT', '종가': 'CLOSE_PRICE', '대비': 'DIFF', '등락률': 'PCT_CHG',
                                        '시가': 'OPEN_PRICE', '고가': 'HIGH_PRICE', '저가': 'LOW_PRICE', '거래량': 'TRADE_VOL', '거래대금': 'TRANS_AMT', '시가총액': 'MKT_CAP', '상장주식수': 'LISTED_SHARES_NUM'})
                elif menu == 2:    
                    df = df.rename(columns={'종목코드': 'ITEM_CODE', '종목명': 'ITEM_NAME', '시장구분': 'MARKET_DIV', '업종명': 'INDUSTRY_NAME', '종가': 'CLOSE_PRICE', '대비': 'DIFF', '등락률': 'PCT_CHG', '시가총액': 'MKT_CAP'})
                elif menu == 3:
                    if i == 0:
                        df['MARKET_DIV'] = 'KRX 300'
                    elif i == 1:
                        df['MARKET_DIV'] = 'KOSDAQ 150'
                    else:
                        df['MARKET_DIV'] = 'KOSPI 200'
                    df = df.rename(columns={'종목코드': 'ITEM_CODE', '종목명': 'ITEM_NAME', '종가': 'CLOSE_PRICE', '대비': 'DIFF', '등락률': 'PCT_CHG', '상장시가총액': 'MKT_CAP'})
                elif menu == 4:
                    df = df.rename(columns={'종목코드': 'ITEM_CODE', '종목명': 'ITEM_NAME', '종가': 'CLOSE_PRICE', '대비': 'DIFF', '등락률': 'PCT_CHG', '상장주식수': 'LISTED_SHARES', '외국인 보유수량': 'FOREIGNER_OWN', '외국인 지분율': 'FOREIGNER_RATIO', '외국인 한도수량': 'FOREIGNER_LIMIT', '외국인 한도소진율': 'FOREIGNER_EXHAUST'})
                    df = df[['BASE_DATE', 'ITEM_CODE', 'ITEM_NAME', 'CLOSE_PRICE', 'DIFF', 'PCT_CHG', 'LISTED_SHARES', 'FOREIGNER_OWN', 'FOREIGNER_RATIO', 'FOREIGNER_LIMIT', 'FOREIGNER_EXHAUST']]
                elif menu == 5:
                    df = df.rename(columns={'종목코드': 'ITEM_CODE', '종목명': 'ITEM_NAME', '종가': 'CLOSE_PRICE', '대비': 'DIFF', '등락률': 'PCT_CHG', '선행 EPS': 'PRE_EPS', '선행 PER': 'PRE_PER', '주당배당금': 'DIV_PER_SHARE', '배당수익률': 'DIV_YIELD'})
                    df = df[['BASE_DATE', 'ITEM_CODE', 'ITEM_NAME', 'CLOSE_PRICE', 'DIFF', 'PCT_CHG', 'EPS', 'PER', 'PRE_EPS', 'PRE_PER', 'BPS', 'PBR', 'DIV_PER_SHARE', 'DIV_YIELD']]
                elif menu == 6:
                    df = df.rename(columns={'적용일': 'BASE_DATE', '종목코드': 'ITEM_CODE','종목명': 'ITEM_NAME', '대용가격': 'SUBSTITUTE', '산출기준일': 'PROD_DATE', '산출기준일 종가': 'CLOSE_PROD'})
                    df = df[['BASE_DATE', 'ITEM_CODE', 'ITEM_NAME', 'SUBSTITUTE', 'PROD_DATE', 'CLOSE_PROD']]
                elif menu == 7:
                    df = df[['BASE_DATE','종목코드','종목명','거래량_매도','거래량_매수','거래량_순매수','거래대금_매도','거래대금_매수','거래대금_순매수','INVESTOR']]
                    df = df.rename(columns={'종목코드': 'ITEM_CODE', '종목명': 'ITEM_NAME', '거래량_매도': 'SELL_AMT', '거래량_매수': 'BUY_AMT', '거래량_순매수': 'NETBUY_AMT', 
                                            '거래대금_매도': 'SELL_KRW', '거래대금_매수': 'BUY_KRW', '거래대금_순매수': 'NETBUY_KRW'})
                elif menu == 8:
                    df = df[['BASE_DATE','MARKET_DIV','지수명','종가','대비','등락률','시가','고가','저가','거래량','거래대금','상장시가총액']]
                    df = df.rename(columns={'지수명': 'IDX_NAME', '종가': 'CLOSE_PRICE', '대비': 'DIFF', '등락률': 'PCT_CHG', '시가': 'OPEN_PRICE', '고가': 'HIGH_PRICE', '저가': 'LOW_PRICE', '거래량': 'TRADE_VOL', '거래대금': 'TRANS_AMT', '상장시가총액': 'MKT_CAP'})
                    try:
                        df['DIFF'] = df['DIFF'].str.replace(',', '').astype(float)
                    except:
                        a = 1
                elif menu == 9:
                    df = df[['BASE_DATE','MARKET_DIV','지수명','종가','대비','등락률','PER','PBR','배당수익률']]
                    df = df.rename(columns={'지수명': 'IDX_NAME', '종가': 'CLOSE_PRICE', '대비': 'DIFF', '등락률': 'PCT_CHG', '배당수익률': 'DIV_YIELD'})
                
                con_df = pd.concat([con_df, df], ignore_index=True)
        new_df = pd.concat([new_df, con_df], ignore_index=True)
    
    new_df.to_csv(f"./{filename}.csv", index=False)

if __name__ == "__main__":
    print("######   Merging CSV files   ######")
    print("Which data do you want to merge?\n1. KRX_OHLCV\n2. KRX_INDUSTRY_CLASSIFICATION\n3. KRX_INDEX_COMPOSITION\n4. KRX_FOREIGNER_HOLD\n5. KRX_PER_PBR_DIV\n6. KRX_STOCK_SUBSTITUTE\n7. KRX_INVESTOR_BUY_TOP\n8. KRX_IDX_PRICE\n9. KRX_IDX_PER_PBR_DIV")
    menu = int(input())
    paths = []
    if menu == 1 or (menu >= 4 and menu <= 9):
        path = input("Input folder path: ")
        paths.append(path)
    elif menu == 2:
        path = input("Input KOSPI folder path: ")
        paths.append(path)
        path = input("Input KOSDAQ folder path: ")
        paths.append(path)
    elif menu == 3:
        path = input("Input KRX 300 folder path: ")
        paths.append(path)
        path = input("Input KOSDAQ 150 folder path: ")
        paths.append(path)
        path = input("Input KOSPI 200 folder path: ")
        paths.append(path)
    else:
        print("Wrong number!!")
        exit()
    filename = input("Input new file name: ")
    merge(menu, paths, filename)