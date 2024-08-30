# This project is for crawling stock data from several sources, inserting into own DB, and adjusting some data.

## Description
Each programs can be used separately.
Also, if you run daily.py, it automatically crawls data and inserts into DB.
All descriptions are written in code in Korean.


Description for arguments in crawl/adjustprice/adjust.py

-input: The name of csv file which contains tickers that you want to know adjusted price. If name of the file is like 'a.csv', you need to input just 'a'. Tickers in csv file must be contained in 'ITEM_CODE' column. If you don't input anything in this field, the program will read KRXlist.csv, which contains all KRX tickers.

-output: The name of csv file which you want to decide as output file name. If name of the file is like 'a.csv', you need to input just 'a'. If you don't input anything in this field, output file name will be decided as 'output.csv'.

-start_date: The starting date you want to adjust price. Format of this field must be str, so you need to input like '"2018-03-18"'. If you don't input anything in this field, start date will be decided as 2000-01-01, which specifies the maximum date.

Example of running adjust.py >> python3 adjust.py -input test_input -output test_output -start_date "2017-01-01"


Description for arguments in crawl/score/score.py

-input: The name of csv file which contains tickers that you want to know adjusted price. If name of the file is like 'a.csv', you need to input just 'a'. Tickers in csv file must be contained in 'ITEM_CODE' column. If you don't input anything in this field, the program will read KOSPI200list.csv, which contains all KOSPI 200 tickers.

-output: The name of csv file which you want to decide as output file name. If name of the file is like 'a.csv', you need to input just 'a'. If you don't input anything in this field, output file name will be decided as 'result.csv'.

-start_date: The starting date you want to adjust price. Format of this field must be str, so you need to input like '"2018-03-18"'. If you don't input anything in this field, start date will be decided as 2001-01-02, which specifies the maximum date.

-end_date: The ending date you want to adjust price. Format of this field must be str, so you need to input like '"2018-03-18"'. If you don't input anything in this field, start date will be decided as 2024-07-30.

Example of running score.py >> python3 score.py -input test_input -output test_output -start_date "2017-01-01" -end_date "2017-01-31"

## Roadmap
There is some bug in daily.py, which doesn't automatically start at designated time. I don't have any idea about the reason of this bug, but I have three expectations: 1. Because of poor performance of our server computer, 2. Because of the error of schedule library, 3. Because of the issue with prompt not running properly when minimized.

There is several error data in Yahoo finance especially in Korean stock splits and dividends data, so the data need to be looked at carefully.