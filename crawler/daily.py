from krx.insert import insert_krx
from krx.merge import merge
from krx.crawl import make_csv
from yahoofinance.insert import insert_yahoo
from yahoofinance.crawl import crawl
from time import sleep
import datetime
import os
import schedule
import pymysql
from dotenv import load_dotenv
import shutil

def main_krx(start_date, end_date):
    """KRX 페이지에서 하루의 주가 데이터를 자동으로 크롤링하여 DB에 insert하는 함수

    Args:
        start_date(datetime.date()): 어느 날짜부터 DB에 넣을지 알려주는 Argument.
        end_date(datetime.date()): 어느 날짜까지 DB에 넣을지 알려주는 Argument.

    !!!
    126서버가 사양이 좋지 않아서 그런지는 잘 모르겠으나 가끔씩 지정된 시간에 자동으로
    프로그램이 시작되지 않는 버그가 존재. 그럴때는 프로그램이 돌아가는 프롬프트 창을 누르고
    화살표만 눌러주면 자동으로 시작된다.
    !!!
    """    
    print("Crawling started")
    make_csv(1, start_date, end_date, "daily/KRX/a")
    make_csv(2, start_date, end_date, "daily/KRX/b/KOSPI")
    make_csv(3, start_date, end_date, "daily/KRX/b/KOSDAQ")
    make_csv(4, start_date, end_date, "daily/KRX/c/KRX 300")
    make_csv(5, start_date, end_date, "daily/KRX/c/KOSDAQ 150")
    make_csv(6, start_date, end_date, "daily/KRX/c/KOSPI 200")
    make_csv(7, start_date, end_date, "daily/KRX/d")
    make_csv(8, start_date, end_date, "daily/KRX/e")
    make_csv(9, start_date, end_date, "daily/KRX/f")
    make_csv(10, start_date, end_date, "daily/KRX/g/1000")
    make_csv(11, start_date, end_date, "daily/KRX/g/2000")
    make_csv(12, start_date, end_date, "daily/KRX/g/3000")
    make_csv(13, start_date, end_date, "daily/KRX/g/3100")
    make_csv(14, start_date, end_date, "daily/KRX/g/4000")
    make_csv(15, start_date, end_date, "daily/KRX/g/5000")
    make_csv(16, start_date, end_date, "daily/KRX/g/6000")
    make_csv(17, start_date, end_date, "daily/KRX/g/7050")
    make_csv(18, start_date, end_date, "daily/KRX/g/7100")
    make_csv(19, start_date, end_date, "daily/KRX/g/8000")
    make_csv(20, start_date, end_date, "daily/KRX/g/9000")
    make_csv(21, start_date, end_date, "daily/KRX/g/9001")
    make_csv(22, start_date, end_date, "daily/KRX/g/9999")
    make_csv(23, start_date, end_date, "daily/KRX/h/KRX")
    make_csv(24, start_date, end_date, "daily/KRX/h/KOSPI")
    make_csv(25, start_date, end_date, "daily/KRX/h/KOSDAQ")
    make_csv(26, start_date, end_date, "daily/KRX/h/THEME")
    make_csv(27, start_date, end_date, "daily/KRX/i/KRX")
    make_csv(28, start_date, end_date, "daily/KRX/i/KOSPI")
    make_csv(29, start_date, end_date, "daily/KRX/i/KOSDAQ")
    make_csv(30, start_date, end_date, "daily/KRX/i/THEME")
    print("Crawling finished")

    print("Merging started")
    if not os.path.exists(f"daily/KRX/{str(end_date)}"):
        os.makedirs(f'daily/KRX/{str(end_date)}')
    paths = []
    paths.append("daily/KRX/a")
    merge(1, paths, f"daily/KRX/{str(end_date)}/a")
    paths = []
    paths.append("daily/KRX/b/KOSPI")
    paths.append("daily/KRX/b/KOSDAQ")
    merge(2, paths, f"daily/KRX/{str(end_date)}/b")
    paths = []
    paths.append("daily/KRX/c/KRX 300")
    paths.append("daily/KRX/c/KOSDAQ 150")
    paths.append("daily/KRX/c/KOSPI 200")
    merge(3, paths, f"daily/KRX/{str(end_date)}/c")
    paths = []
    paths.append("daily/KRX/d")
    merge(4, paths, f"daily/KRX/{str(end_date)}/d")
    paths = []
    paths.append("daily/KRX/e")
    merge(5, paths, f"daily/KRX/{str(end_date)}/e")
    paths = []
    paths.append("daily/KRX/f")
    merge(6, paths, f"daily/KRX/{str(end_date)}/f")
    paths = []
    paths.append("daily/KRX/g")
    merge(7, paths, f"daily/KRX/{str(end_date)}/g")
    paths = []
    paths.append("daily/KRX/h")
    merge(8, paths, f"daily/KRX/{str(end_date)}/h")
    paths = []
    paths.append("daily/KRX/i")
    merge(9, paths, f"daily/KRX/{str(end_date)}/i")
    print("Merging finished")

    l = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i']

    for i in l:
        if os.path.exists(f"daily/KRX/{i}"):
            shutil.rmtree(f"daily/KRX/{i}")
    
    print("Inserting started")
    try:
        insert_krx(1, f"daily/KRX/{str(end_date)}/a.csv")
        insert_krx(2, f"daily/KRX/{str(end_date)}/b.csv")
        insert_krx(3, f"daily/KRX/{str(end_date)}/c.csv")
        insert_krx(4, f"daily/KRX/{str(end_date)}/d.csv")
        insert_krx(5, f"daily/KRX/{str(end_date)}/e.csv")
        insert_krx(6, f"daily/KRX/{str(end_date)}/f.csv")
        insert_krx(7, f"daily/KRX/{str(end_date)}/g.csv")
        insert_krx(8, f"daily/KRX/{str(end_date)}/h.csv")
        insert_krx(9, f"daily/KRX/{str(end_date)}/i.csv")
        print("Inserting finished")
    except:
        print("No data to insert")

def main_yahoo(start_date, end_date, menu):
    """Yahoo finance에서 하루의 주가 데이터를 자동으로 크롤링하여 DB에 insert하는 함수

    Args:
        start_date(datetime.date()): 어느 날짜부터 DB에 넣을지 알려주는 Argument.
        end_date(datetime.date()): 어느 날짜까지 DB에 넣을지 알려주는 Argument.
        menu(int): 한국장 데이터를 넣을지, 미국장 데이터를 넣을지 정하는 Argument.
    """
    print("Crawling started")
    if not os.path.exists(f'./daily/Yahoo/{str(end_date)}'):
        os.makedirs(f'./daily/Yahoo/{str(end_date)}')
    if menu == 1:
        crawl(start_date, end_date, "KOSDAQ150")
        crawl(start_date, end_date, "KOSPI200")
    elif menu == 2:
        crawl(start_date, end_date, "NASDAQ100")
        crawl(start_date, end_date, "S&P500")
    print("Crawling finished")
    
    print("Inserting started")
    try:
        if menu == 1:
            insert_yahoo(f"daily/Yahoo/{str(end_date)}/KOSDAQ150", "YAHOO")
            insert_yahoo(f"daily/Yahoo/{str(end_date)}/KOSPI200", "YAHOO")
        elif menu == 2:
            insert_yahoo(f"daily/Yahoo/{str(end_date)}/NASDAQ100", "YAHOO")
            insert_yahoo(f"daily/Yahoo/{str(end_date)}/S&P500", "YAHOO")
        print("Inserting finished")
    except:
        print("No data to insert")

def main(menu):
    """매일 06시 30분과 16시 30분마다 DB에 지금까지 들어있는 데이터의 마지막 날짜 다음날부터 당일까지 크롤링하여 DB에 insert하는 함수.

    Args:
        menu(int): 한국장 데이터를 넣을지, 미국장 데이터를 넣을지 정하는 Argument.
    """
    print("started at: " + str(datetime.datetime.now()))

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
        # KRX data 중 DB에 들어있는 가장 마지막 날짜를 불러온다.
        query = f"""
            SELECT BASE_DATE FROM KRX_OHLCV ORDER BY BASE_DATE DESC LIMIT 1;
            """

        mycursor.execute(query)

        result = mycursor.fetchall()
        start_date = result[0][0]
        start_date += datetime.timedelta(days=1)
        end_date = datetime.datetime.now().date()

        print("start crawling KRX")

        main_krx(start_date, end_date)
        
    if menu == 1:
        tmp = ''
    elif menu == 2:
        tmp = ' WHERE MARKET_DIV = \'S&P 500\''
    # yahoo data 중 DB에 들어있는 가장 마지막 날짜를 불러온다.
    query = f"""
        SELECT BASE_DATE FROM YAHOO{tmp} ORDER BY BASE_DATE DESC LIMIT 1;
        """

    mycursor.execute(query)

    result = mycursor.fetchall()
    start_date = result[0][0]
    start_date += datetime.timedelta(days=1)
    end_date = datetime.datetime.now().date()

    print("start crawling yahoo")

    main_yahoo(start_date, end_date, menu)

    print("finished at: " + str(datetime.datetime.now()))


if __name__ == '__main__':
    sched = schedule.every().day.at("16:30").do(main, 1)  # 여기서 시간을 바꾸면 지정된 시간에 크롤링하여 insert 가능
    sched2 = schedule.every().day.at("06:30").do(main, 2)  # Yahoo의 미국장을 넣기 위한 함수
    try:
        while True:
            schedule.run_pending()
            sleep(1)
    except (KeyboardInterrupt, SystemExit):
        schedule.cancel_job(sched)
        schedule.cancel_job(sched2)
        exit()