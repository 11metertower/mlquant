import yfinance as yf
import pandas as pd
import datetime
import os

def crawl(start_date, end_date, folder_name):
    """Yahoo finance에서 API를 통해 데이터들을 가져올 수 있는 yfinance라는 library를 통해
    start_date부터 end_date까지의 주가 데이터를 받아와 folder_name 폴더 안에 저장하는 함수

    Args:
        start_date (datetime.date): 주가 데이터를 가져올 시작 날짜를 지정한 변수
        end_date (datetime.date): 주가 데이터를 가져올 마지막 날짜를 지정한 변수
        folder_name (str): 주가 데이터들을 저장할 폴더 이름을 지정한 변수
    """
    if not os.path.exists(f'./daily/Yahoo/{str(end_date)}/{folder_name}'):
        os.makedirs(f'./daily/Yahoo/{str(end_date)}/{folder_name}')
    table = pd.read_csv(f"{folder_name}list.csv", dtype=object)  # 미리 수집된 각 시장들의 2000년대의 역대 종목들의 리스트를 가져온다.
    table = table.values.tolist()

    for ticker, name in table:
        if folder_name == "KOSPI200":  # Yahoo finance의 경우 티커를 변경해줘야 하는데, KOSPI는 뒤에 .KS, KOSDAQ는 뒤에 .KQ를 붙여줘야 함
            real_ticker = ticker + ".KS"
        elif folder_name == "KOSDAQ150":
            real_ticker = ticker + ".KQ"
        else:
            real_ticker = ticker
        try:
            new_data = yf.download(real_ticker, start=str(start_date), end=str(end_date + datetime.timedelta(days=1)))
        except:
            continue  # 해당 티커에 대한 데이터가 없으면 다음 티커로 넘어간다.

        if new_data.empty:
            continue

        info = yf.Ticker(real_ticker).info

        new_data = new_data.rename_axis('Date').reset_index()
        new_data['ITEM_CODE'] = ticker
        new_data['ITEM_NAME'] = name

        try:  # API에서 받아온 정보에 통화 정보가 있으면 그것을 가져오고,
            currency = info['currency']
        except:  # 없으면 시장 정보에 따라 통화 정보를 입력한다.
            print(real_ticker)
            if folder_name == "KOSPI200" or folder_name == "KOSDAQ150":
                currency = "KRW"
            else:
                currency = "USD"

        new_data['CCY'] = currency

        if folder_name == "KOSPI200":
            new_data['MARKET_DIV'] = "KOSPI 200"
        elif folder_name == "KOSDAQ150":
            new_data['MARKET_DIV'] = "KOSDAQ 150"
        elif folder_name == "NASDAQ100":
            new_data['MARKET_DIV'] = "NASDAQ 100"
        else:
            new_data['MARKET_DIV'] = "S&P 500"
        
        new_data = new_data.round(4)  # 모든 데이터는 소숫점 5번째 자리에서 반올림한다.

        new_data.to_csv(f"./daily/Yahoo/{str(end_date)}/{folder_name}/{ticker}.csv", index=False)


if __name__ == "__main__":
    start_year = int(input("Input year of start date: "))
    start_month = int(input("Input month of start date: "))
    start_day = int(input("Input day of start date: "))
    start_date = datetime.date(start_year, start_month, start_day)

    end_year = int(input("Input year of end date: "))
    end_month = int(input("Input month of end date: "))
    end_day = int(input("Input day of end date: "))
    end_date = datetime.date(end_year, end_month, end_day)

    crawl(start_date, end_date, "KOSDAQ150")
    crawl(start_date, end_date, "KOSPI200")
    crawl(start_date, end_date, "NASDAQ100")
    crawl(start_date, end_date, "S&P500")