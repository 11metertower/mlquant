import requests as rq
from io import BytesIO
import pandas as pd
from tqdm import tqdm
import datetime
import os
import random
from time import sleep

def inputs():
    """크롤링을 할 시작 날짜, 마지막 날짜, 그리고 저장할 folder의 이름을 받는 함수

    Returns:
        시작 날짜, 마지막 날짜 2개는 받아서 datetiem.date로 변환하여 return하고,
        folder_name은 받아서 str 그대로 return한다.
    """
    start_year = int(input("Input year of start date: "))
    start_month = int(input("Input month of start date: "))
    start_day = int(input("Input day of start date: "))
    start_date = datetime.date(start_year, start_month, start_day)

    end_year = int(input("Input year of end date: "))
    end_month = int(input("Input month of end date: "))
    end_day = int(input("Input day of end date: "))
    end_date = datetime.date(end_year, end_month, end_day)

    folder_name = input("Input folder name: ")

    return start_date, end_date, folder_name

def date_to_int(d):
    """datetime.date을 yyyymmdd 형식의 int로 바꿔주는 함수

    Args:
        d (datetime.date): int로 바꿀 date
    
    Returns:
        int: d를 yyyymmdd 형식의 int로 바꿔서 return
    """
    return d.year*10000 + d.month*100 + d.day

def make_csv(menu, start_date, end_date, folder_name):
    """크롤링을 해서 지정된 폴더 내에 {날짜}.csv로 저장해주는 함수

    Args:
        menu (int): 어떤 데이터를 크롤링할지 그 데이터를 지정하는 변수
                    1: 전 종목 시세, 2: 업종 분류 현황 -> KOSPI, 3: 업종 분류 현황 -> KOSDAQ,
                    4: 지수 구성 종목 -> KRX 300, 5: 지수 구성 종목 -> KOSDAQ 150, 6: 지수 구성 종목 -> KOSPI 200    
        start_date: 크롤링을 할 시작 날짜
        end_date: 크롤링을 할 마지막 날짜
        folder_name: 크롤링을 한 데이터들을 저장할 폴더 이름
    """     
    if not os.path.exists(f'./{folder_name}'):
        os.makedirs(f'./{folder_name}')

    if menu == 4 or menu == 5 or menu == 6:
        if menu == 4:
            equid = 'KRX 300'
            indIdx = '5'
            indIdx2 = '300'
        elif menu == 5:
            equid = '코스닥 150'
            indIdx = '2'
            indIdx2 = '203'
        else:
            equid = '코스피 200'
            indIdx = '1'
            indIdx2 = '028'
        gen_otp = {
            'locale': 'ko_KR',
            'tboxindIdx_finder_equidx0_2': equid,
            'indIdx': indIdx,
            'indIdx2': indIdx2,
            'codeNmindIdx_finder_equidx0_2': equid,
            'param1indIdx_finder_equidx0_2': '',
            'trdDd': '',
            'money': '3',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT00601'
        }
    elif menu == 1:
        mktId = 'ALL'
        gen_otp = {
            'locale': 'ko_KR',
            'mktId': mktId,
            'trdDd': '',
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT01501'
        }
    elif menu == 2 or menu == 3:
        if menu == 2:
            mktId = 'STK'
        else:
            mktId = 'KSQ'
        gen_otp = {
            'locale': 'ko_KR',
            'mktId': mktId,
            'trdDd': '',
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
        }
    elif menu == 7:
        gen_otp = {
            'locale': 'ko_KR',
            'searchType': '1',
            'mktId': 'ALL',
            'trdDd': '',
            'tboxisuCd_finder_stkisu0_1': '',
            'isuCd': 'KR7000640003',
            'isuCd2': 'KR7005930003',
            'codeNmisuCd_finder_stkisu0_1': '',
            'param1isuCd_finder_stkisu0_1': 'ALL',
            'strtDd': '',
            'endDd': '',
            'share': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT03701'
        }
    elif menu == 8:
        gen_otp = {
            'locale': 'ko_KR',
            'searchType': '1',
            'mktId': 'ALL',
            'trdDd': '',
            'tboxisuCd_finder_stkisu0_0': '',
            'isuCd': 'KR7000640003',
            'isuCd2': 'KR7005930003',
            'codeNmisuCd_finder_stkisu0_0': '',
            'param1isuCd_finder_stkisu0_0': 'ALL',
            'strtDd': '',
            'endDd': '',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT03501'
        }
    elif menu == 9:
        gen_otp = {
            'locale': 'ko_KR',
            'searchType': '1',
            'mktId': 'ALL',
            'trdDd': '',
            'tboxisuCd_finder_stkisu0_0': '',
            'isuCd': 'KR7000640003',
            'isuCd2': 'KR7005930003',
            'codeNmisuCd_finder_stkisu0_0': '',
            'param1isuCd_finder_stkisu0_0': 'ALL',
            'strtDd': '',
            'endDd': '',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT04001'
        }
    elif menu >= 10 and menu <= 22:
        gen_otp = {
            'locale': 'ko_KR',
            'mktId': 'ALL',
            'invstTpCd': '',
            'strtDd': '',
            'endDd': '',
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT02401'
        }
    elif menu >= 23 and menu <= 26:
        gen_otp = {
            'locale': 'ko_KR',
            'idxIndMidclssCd': f'0{menu-22}',
            'trdDd': '',
            'share': '2',
            'money': '3',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT00101'
        }
    elif menu >= 27 and menu <= 30:
        gen_otp = {
            'locale': 'ko_KR',
            'searchType': 'A',
            'idxIndMidclssCd': f'0{menu-26}',
            'trdDd': '',
            'tboxindTpCd_finder_equidx0_1': '',
            'indTpCd': '',
            'indTpCd2': '',
            'codeNmindTpCd_finder_equidx0_1': '',
            'param1indTpCd_finder_equidx0_1': '',
            'strtDd': '',
            'endDd': '',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT00701'
        }

    gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader', 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'}
    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'

    for delta in tqdm(range((end_date - start_date).days + 1)):
        date = str(date_to_int(start_date + datetime.timedelta(days=delta)))
        gen_otp['trdDd'] = date

        if menu >= 10 and menu <= 22:
            gen_otp['strtDd'] = date
            gen_otp['endDd'] = date
            if menu >= 10 and menu <= 12:
                gen_otp['invstTpCd'] = str((menu-9)*1000)
            elif menu == 13:
                gen_otp['invstTpCd'] = '3100'
            elif menu == 14:
                gen_otp['invstTpCd'] = '4000'
            elif menu == 15:
                gen_otp['invstTpCd'] = '5000'
            elif menu == 16:
                gen_otp['invstTpCd'] = '6000'
            elif menu == 17:
                gen_otp['invstTpCd'] = '7050'
            elif menu == 18:
                gen_otp['invstTpCd'] = '7100'
            elif menu == 19:
                gen_otp['invstTpCd'] = '8000'
            elif menu == 20:
                gen_otp['invstTpCd'] = '9000'
            elif menu == 21:
                gen_otp['invstTpCd'] = '9001'
            elif menu == 22:
                gen_otp['invstTpCd'] = '9999'
        
        otp = rq.post(gen_otp_url, gen_otp, headers=headers).text
        
        down_sector = rq.post(down_url, {'code': otp}, headers=headers)
        
        sector = pd.read_csv(BytesIO(down_sector.content), encoding='EUC-KR')
        
        if (menu == 8 or menu == 1 or menu == 2 or menu == 3 or (menu >= 27 and menu <= 30)) and pd.isna(sector['종가']).any():
            continue

        if menu >= 23 and menu <= 26:
            summ = pd.isna(sector['종가']).sum()
            if menu == 23 and summ == 28:
                continue
            elif menu == 24 and summ == 47:
                continue
            elif menu == 25 and summ == 38:
                continue
            elif menu == 26 and summ == 33:
                continue
        
        if menu >= 23 and menu <= 30:
            if menu == 23 or menu == 27:
                sector['MARKET_DIV'] = 'KRX'
            elif menu == 24 or menu == 28:
                sector['MARKET_DIV'] = 'KOSPI'
            elif menu == 25 or menu == 29:
                sector['MARKET_DIV'] = 'KOSDAQ'
            elif menu == 26 or menu == 30:
                sector['MARKET_DIV'] = 'THEME'

        if menu == 9:
            sector['적용일'] = sector['적용일'].str.replace('/', '-')
        
        if menu >= 10 and menu <= 22:
            sector['INVESTOR'] = gen_otp['invstTpCd']

        try:
            if sector.empty:
                continue
        except:
            continue

        sector.to_csv(f"./{folder_name}/{date}.csv", index=False)
        sleep(random.randrange(2, 6))
        


print('''
                  ,------------,                  
              ----------------------              
           ----------------------------           
         --------------------------------         
       ----------------,-------------------       
     -----------,,,---, ,--------------------     
    ------------,  ,--  .---------------------    
   --------------   -,   ,,,,,,,---------------   
  ------------,,,   ,          .,---------------  
 ,----------,,.      .......     ---------------, 
 -----------,....    .------     ---------------- 
,---------------,     -----.    .----------------,
-----------------.    .....     ...---------------
-----------------,                 .--------------
------------------     ....,,..     .-------------
------------------.    .-------      -------------
.------------------     ------.     .------------.
 ------------------.    ......     .------------- 
 .---------------...           ,,,,-------------. 
  ---------------         ,,   -----------------  
   --------------.,,,,,   --   ----------------   
    -------------,-----,  .-,,,---------------    
      ------------------,,,-----------------      
       ------------------------------------       
         --------------------------------         
           ,--------------------------,           
              ,--------------------,              
                   .----------.                   
''')

def main():
    print("######   Stock to CSV exchanger   ######")
    while(True):
        menu = int(input("1. 전 종목 시세\n2. 업종 분류 현황\n3. 지수 구성 종목\n4. 외국인 보유량\n5. PER/PBR/배당수익률\n6. 주식대용가\n7. 투자자별 순매수상위종목\n8. 전체지수시세\n9. PER/PBR/배당수익률 지수\n10. exit\n"))
        if menu == 1:
            start_date, end_date, folder_name = inputs()
            make_csv(1, start_date, end_date, folder_name)
        elif menu == 2:
            menu2 = int(input("1. KOSPI\n2. KOSDAQ\n"))
            if menu2 == 1 or menu2 == 2:
                start_date, end_date, folder_name = inputs()
                make_csv(menu2 + 1, start_date, end_date, folder_name)
            else:
                print("Your input is wrong!!")
                continue
        elif menu == 3:
            menu2 = int(input("1. KRX 300\n2. KOSDAQ 150\n3. KOSPI 200\n"))
            if menu2 == 1 or menu2 == 2 or menu2 == 3:
                start_date, end_date, folder_name = inputs()
                make_csv(menu2 + 3, start_date, end_date, folder_name)
            else:
                print("Your input is wrong!!")
                continue
        elif menu == 4 or menu == 5 or menu == 6:
            start_date, end_date, folder_name = inputs()
            make_csv(menu + 3, start_date, end_date, folder_name)
        elif menu == 7:
            menu2 = int(input("1. 금융투자\n2. 보험\n3. 투신\n4. 사모\n5. 은행\n6. 기타금융\n7. 연기금 등\n8. 기관합계\n9. 기타법인\n10. 개인\n11. 외국인\n12. 기타외국인\n13. 전체\n"))
            if menu2 >= 1 and menu2 <= 13:
                start_date, end_date, folder_name = inputs()
                make_csv(menu2 + 9, start_date, end_date, folder_name)
            else:
                print("Your input is wrong!!")
                continue
        elif menu == 8:
            menu2 = int(input("1. KRX\n2. KOSPI\n3. KOSDAQ\n4. 테마\n"))
            if menu2 >= 1 and menu2 <= 4:
                start_date, end_date, folder_name = inputs()
                make_csv(menu2 + 22, start_date, end_date, folder_name)
            else:
                print("Your input is wrong!!")
                continue
        elif menu == 9:
            menu2 = int(input("1. KRX\n2. KOSPI\n3. KOSDAQ\n4. 테마\n"))
            if menu2 >= 1 and menu2 <= 4:
                start_date, end_date, folder_name = inputs()
                make_csv(menu2 + 26, start_date, end_date, folder_name)
            else:
                print("Your input is wrong!!")
                continue
        elif menu == 10:
            break
        else:
            print("Your input is wrong!!")

if __name__ == "__main__":
    main()