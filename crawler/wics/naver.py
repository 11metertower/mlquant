from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

def crawl(ticker, driver):
    """naver finance 홈페이지에서 크롤링하는 함수.
    
    Args:
        ticker: 크롤링할 종목의 ticker
        driver: 크롤링에 필요한 chrome driver
    """
    # 웹페이지 열기
    url = f'https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={ticker}'
    driver.get(url)

    # 명시적 대기를 사용해 특정 요소가 로드될 때까지 대기 (최대 10초)
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'dt.line-left'))
        )
    except Exception as e:
        return 'No data', 'No data', 'No data'

    # 페이지 소스 가져오기
    page_source = driver.page_source

    # BeautifulSoup으로 파싱
    soup = BeautifulSoup(page_source, 'html.parser')

    # 모든 dt 태그와 line-left 클래스를 가진 요소들을 찾음
    data_sections = soup.find_all('dt', class_='line-left')

    # 각 섹션의 텍스트를 출력
    for idx, section in enumerate(data_sections, start=1):
        data_text = section.get_text(separator=" ", strip=True)
        if 'KOSPI :' in data_text:
            data_text = data_text.split(' ')[2]
            ret1 = 'KOSPI'
            ret2 = data_text
        elif 'KOSDAQ :' in data_text:
            print("KOSDAQ: ", ticker)
            data_text = data_text.split(' ')[2]
            ret1 = 'KOSDAQ'
            ret2 = data_text
        if 'WICS :' in data_text:
            data_text = data_text.split(' ')[2]
            ret3 = data_text

    return ret1, ret2, ret3

def main():
    """naver finance 홈페이지에서 미리 지정된 종목들에 따른 wics 분류를 가져오는 프로그램
    """
    # Chrome 옵션 설정
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # 브라우저 창을 띄우지 않음
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')

    # ChromeDriver 자동 설치 및 설정
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    df = pd.read_csv("tickers.csv", dtype=object)
    l = df['ITEM_CODE'].tolist()

    for i, ticker in tqdm(enumerate(l)):
        try:
            df.loc[i, 'MARKET_DIV'], df.loc[i, 'DIVISION'], df.loc[i, 'WICS'] = crawl(ticker, driver)
        except:
            df.loc[i, 'MARKET_DIV'], df.loc[i, 'DIVISION'], df.loc[i, 'WICS'] = 'No data', 'No data', 'No data'

    df["MARKET_DIV"] = df['MARKET_DIV'].str.replace("No data", "")
    df["DIVISION"] = df['DIVISION'].str.replace("No data", "")
    df["WICS"] = df['WICS'].str.replace("No data", "")
    df.to_csv("result_naver.csv", index=False)
    # 브라우저 닫기
    driver.quit()

if __name__ == '__main__':
    main()