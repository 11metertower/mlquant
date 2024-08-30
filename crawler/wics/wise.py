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
import json
global idx
idx = 0
df = pd.DataFrame()
def crawl(date, wics, driver):
    """wise index 홈페이지에서 크롤링하는 함수.
    
    Args:
        date: 크롤링할 날짜
        wics: wics 중분류 코드
        driver: 크롤링에 필요한 chrome driver
    """
    global idx
    # 웹페이지 열기
    url = f'https://www.wiseindex.com/Index/GetindexComponets?ceil_yn=0&dt={date}&sec_cd=g{wics}'
    driver.get(url)

    # 명시적 대기를 사용해 특정 요소가 로드될 때까지 대기 (최대 1초)
    try:
        WebDriverWait(driver, 1).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'pre'))
        )
    except Exception as e:
        return 2

    # 페이지 소스 가져오기
    page_source = driver.page_source

    # BeautifulSoup으로 파싱
    soup = BeautifulSoup(page_source, 'html.parser')

    # 모든 pre 태그를 가진 요소들을 찾음
    data_sections = soup.find('pre')

    if data_sections == None:
        return 0
    data_text = data_sections.get_text(separator=" ", strip=True)
    data_text = json.loads(data_text)
    data_text = data_text['list']
    if data_text == []:
        return 0
    for data in data_text:
        d = dict(data)
        df.loc[idx, 'BASE_DATE'] = date
        df.loc[idx, 'ITEM_CODE'] = d['CMP_CD']
        df.loc[idx, 'ITEM_NAME'] = d['CMP_KOR']
        df.loc[idx, 'WICS_CODE'] = str(wics)
        df.loc[idx, 'WICS_NAME'] = d['SEC_NM_KOR']
        idx += 1
    return 1

def main():
    """wise index 홈페이지에서 한 달의 첫째날에 wics 중분류에 따른 종목들을 가져오는 프로그램
    """
    # Chrome 옵션 설정
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # 브라우저 창을 띄우지 않음
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')

    # ChromeDriver 자동 설치 및 설정
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    l = ['1010', '1510', '2010', '2020', '2030', '2510', '2520', '2530', '2550', '2560', '3010', '3020', '3030', '3510', '3520', '4010', '4020', '4030', '4040', '4050', '4510', '4520', '4530', '4535', '4540', '5010', '5020', '5510']
    
    for year in tqdm(range(2010, 2025)):  # 여기서 가져올 년도의 범위를 정할 수 있음
        for month in tqdm(range(1, 13)):
            if year == 2024 and month > 8:
                break
            for i in l:
                day = 1
                while True:
                    date = str(year*10000 + month*100 + day)
                    a = crawl(date, i, driver)
                    if a == 0:
                        day += 1
                    elif a == 1:
                        
                        break
                    elif a == 2:
                        break
    df.to_csv("result_wics.csv", index=False)
    # 브라우저 닫기
    driver.quit()

if __name__ == '__main__':
    main()