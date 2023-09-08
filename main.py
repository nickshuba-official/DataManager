# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
from datetime import date
import datedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
import tempfile
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import time
import re
from tabula.io import read_pdf
import random

tickers = [
    "USD000000TOD",
    "USD000TODTOM",
    "EUR_RUB__TOD",
    "EUR000TODTOM",
    "CNY000000TOD",
    "CNYRUBTODTOM"
]

rates_tickers = [
    "RUSFAR",
    "RUSFAR1W",
    "RUSFAR2W",
    "RUSFAR1M",
    "RUSFAR3M",
    "RUSFARCNY"
]


class MoexData:

    def __init__(self, start_dt, tickers, rates_tickers):
        self.start_dt = start_dt
        self.tickers = tickers
        self.rates_tickers = rates_tickers

    def history_quotes_update(self):

        start_dt = self.start_dt
        tickers = self.tickers

        full_data_table = pd.DataFrame()
        for ticker in tickers:
            url = f"https://iss.moex.com/iss/history/engines/currency/markets/" \
                  f"selt/boards/CETS/securities/{ticker}/result.csv?from={start_dt}"
            data = pd.read_csv(
                filepath_or_buffer=url,
                sep=";",
                skiprows=2
            )

            data.rename(columns={
                "BOARDID": "board_id",
                "TRADEDATE": "report_date",
                "SHORTNAME": "short_name",
                "SECID": "sec_id",
                "OPEN": "open_price",
                "LOW": "low_price",
                "HIGH": "high_price",
                "CLOSE": "close_price",
                "NUMTRADES": "num_trades",
                "VOLRUR": "volume_rub",
                "WAPRICE": "wa_price"
            },
                inplace=True)

            data["report_date"] = pd.to_datetime(data["report_date"])
            full_data_table = pd.concat(
                [full_data_table, data],
                ignore_index=True,
                axis=0  # проверить правильность
            )
            time.sleep(random.uniform(0.5, 3))

        full_data_table.drop_duplicates(inplace=True)
        return full_data_table

    def interim_results_update(self):

        tickers = self.tickers

        full_data_table = pd.DataFrame()
        for ticker in tickers:
            url = f"https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/securities/{ticker}/" \
                  f"interim.csv"
            data = pd.read_csv(
                filepath_or_buffer=url,
                sep=";",
                skiprows=7,
                encoding="cp1251"
            ).iloc[:1]
            data.drop(columns={
                "HIGHBID",
                "BIDDEPTH",
                "LOWOFFER",
                "OFFERDEPTH",
                "SPREAD",
                "LASTCNGTOLASTWAPRICE",
                "VALTODAY_USD",
                "WAPTOPREVWAPRICE",
                "CLOSEPRICE",
                "TRADINGSTATUS",
                "UPDATETIME",
                "WAPTOPREVWAPRICEPRCNT",
                "BID",
                "BIDDEPTHT",
                "NUMBIDS",
                "OFFER",
                "OFFERDEPTHT",
                "NUMOFFERS",
                "CHANGE",
                "LASTCHANGEPRCNT",
                "VALUE",
                "VALUE_USD",
                "SEQNUM",
                "QTY",
                "TIME",
                "PRICEMINUSPREVWAPRICE",
                "LASTCHANGE",
                "LASTTOPREVPRICE",
                "VALTODAY_RUR",
                "SYSTIME",
                "MARKETPRICE",
                "MARKETPRICETODAY",
                "MARKETPRICE2",
                "ADMITTEDQUOTE",
                "LOPENPRICE"
            },
                inplace=True
            )

            data.rename(columns={
                "HIGH": "high_price",
                "LOW": "low_price",
                "OPEN": "open_price",
                "LAST": "last_price",
                "VALTODAY": "volume_rub",
                "VOLTODAY": "volume_usd",
                "WAPRICE": "wa_price",
                "NUMTRADES": "num_trades",
                "BOARDID": "board_id",
                "SECID": "sec_id"
            },
                inplace=True)

            data["report_date"] = date.today()
            full_data_table = pd.concat(
                [full_data_table, data],
                ignore_index=True,
                axis=0  # проверить правильность
            )
            time.sleep(random.uniform(0.5, 3))

        full_data_table.drop_duplicates(inplace=True)
        return full_data_table

    def history_rates_update(self):

        start_dt = self.start_dt
        rates_tickers = self.rates_tickers

        full_data_table = pd.DataFrame()
        for ticker in rates_tickers:
            url = f"https://iss.moex.com/iss/history/engines/stock/markets/" \
                  f"index/boards/MMIX/securities/{ticker}/result.csv?from={start_dt}"
            data = pd.read_csv(
                filepath_or_buffer=url,
                sep=";",
                skiprows=2
            )

            data.rename(columns={
                "BOARDID": "board_id",
                "TRADEDATE": "report_date",
                "SHORTNAME": "short_name",
                "SECID": "sec_id",
                "OPEN": "open_price",
                "LOW": "low_price",
                "HIGH": "high_price",
                "CLOSE": "close_price",
                "NUMTRADES": "num_trades",
                "VALUE": "volume_rub",
                "CURRENCYID": "currency"
            },
                inplace=True)
            data.drop(columns={
                "DURATION",
                "YIELD",
                "DECIMALS",
                "CAPITALIZATION",
                "DIVISOR",
                "TRADINGSESSION",
                "VOLUME"
            },
                inplace=True)
            data["report_date"] = pd.to_datetime(data["report_date"])
            full_data_table = pd.concat(
                [full_data_table, data],
                ignore_index=True,
                axis=0  # проверить правильность
            )
            time.sleep(random.uniform(0.5, 3))

        full_data_table.drop_duplicates(inplace=True)
        return full_data_table

    def interim_results_rates_update(self):

        rates_tickers = self.rates_tickers

        full_data_table = pd.DataFrame()
        for ticker in rates_tickers:
            url = f"https://iss.moex.com/iss/engines/stock/markets/index/boards/MMIX/securities/{ticker}/" \
                  f"interim.csv"
            data = pd.read_csv(
                filepath_or_buffer=url,
                sep=";",
                skiprows=7,
                encoding="cp1251"
            ).iloc[:1]
            data.drop(columns={
                'LASTCHANGETOOPENPRC',
                'LASTCHANGETOOPEN',
                'UPDATETIME',
                'LASTCHANGEPRC',
                'MONTHCHANGEPRC',
                'YEARCHANGEPRC',
                'SEQNUM',
                'SYSTIME',
                'TIME',
                'LASTCHANGEBP',
                'MONTHCHANGEBP',
                'YEARCHANGEBP',
                'CAPITALIZATION',
                'CAPITALIZATION_USD',
                'TRADINGSESSION',
                'VOLTODAY'
            },
                inplace=True
            )

            data.rename(columns={
                "BOARDID": "board_id",
                "SECID": "sec_id",
                "LASTVALUE": "yesterday_price",
                "OPENVALUE": "open_price",
                "CURRENTVALUE": "current_price",
                "LASTCHANGE": "daily_change",
                "HIGH": "high_price",
                "LOW": "low_price",
                "VALTODAY": "volume_rub",
                "VALTODAY_USD": "volume_usd",
                "NUMTRADES": "num_trades",
                "TRADEDATE": "report_date"
            },
                inplace=True)

            data["report_date"] = date.today()
            full_data_table = pd.concat(
                [full_data_table, data],
                ignore_index=True,
                axis=0  # проверить правильность
            )
            time.sleep(random.uniform(0.5, 3))

        full_data_table.drop_duplicates(inplace=True)
        return full_data_table

    def zcyc_update(self):

        url = f"https://iss.moex.com/iss/engines/stock/zcyc/result.csv"
        data = pd.read_csv(
            filepath_or_buffer=url,
            sep=";",
            skiprows=56,
            encoding="cp1251"
        )

        data.rename(columns={
            "tradedate": "report_date",
            "tradetime": "report_time",
            "period": "term_years",
            "value": "rate"
        },
            inplace=True)

        data.dropna(
            axis=0,
            how="any",
            inplace=True
        )

        data["report_date"] = pd.to_datetime(data["report_date"], format="%Y-%m-%d")

        return data


def term_years(term_string):
    term_dict = {
        "O/N": 0.00274,
        "1W": 0.019,
        "2W": 0.038,
        "3W": 0.058,
        "1M": 0.083,
        "2M": 0.17,
        "3M": 0.25,
        "4M": 0.33,
        "5M": 0.42,
        "6M": 0.5,
        "9M": 0.75,
        "1Y": 1,
        "2Y": 2,
        "3Y": 3,
        "4Y": 4,
        "5Y": 5,
        "6Y": 6,
        "7Y": 7,
        "8Y": 8,
        "9Y": 9,
        "10Y": 10
    }

    return term_dict[term_string]


start_dt = date.today() - (10 * datedelta.DAY)
# start_dt = date(2023, 7, 1)
moex_data = MoexData(
    start_dt=start_dt,
    tickers=tickers,
    rates_tickers=rates_tickers
)

history_quotes_table = moex_data.history_quotes_update()
interim_results_table = moex_data.interim_results_update()
zcyc_table = moex_data.zcyc_update()
history_rates_table = moex_data.history_rates_update()
interim_results_rates_table = moex_data.interim_results_rates_update()

# roisfix market
roisfix_table = pd.read_html(r"http://roisfix.ru/")[1]
roisfix_table.rename(
    columns={
        0: "term",
        1: "rate"
    },
    inplace=True
)

roisfix_table["term_years"] = roisfix_table["term"].apply(term_years)
roisfix_table["report_date"] = date.today()
roisfix_table["rate_name"] = "roisfix"
roisfix_table.sort_index(
    axis=1,
    inplace=True
)

# roisfix dealers
roisfix_dealers_table = pd.read_html(r"http://roisfix.ru/")[2]
roisfix_dealers_table.set_index(
    keys=roisfix_dealers_table["Bank Code"],
    inplace=True
)
roisfix_dealers_table.drop(
    columns=["Bank Code"],
    inplace=True,
)
roisfix_dealers_table_t = roisfix_dealers_table.transpose()


def correct_roisfix_table(roisfix_dealers_table_t):
    bank_names = {
        "AGRM": "Россельхозбанк",
        "BSPB": "БСПБ",
        "GZPR": "Газпромбанк",
        "MEIN": "Металлинвестбанк",
        "OPEN": "Открытие",
        "RVTB": "ВТБ",
        "RZBM": "Райффайзенбанк",
        "SBER": "Сбер"
    }

    bank_codes = list(roisfix_dealers_table_t.columns)

    roisfix_dealers_table_correct = pd.DataFrame(columns=[])

    for bank_code in bank_codes:
        roisfix_dealer_table = roisfix_dealers_table_t[[bank_code]].copy()
        roisfix_dealer_table["bank_name"] = bank_names[bank_code]
        roisfix_dealer_table["bank_code"] = bank_code
        roisfix_dealer_table.reset_index(inplace=True)
        roisfix_dealer_table.rename(
            columns={bank_code: "rate",
                     "index": "term"},
            inplace=True
        )
        roisfix_dealers_table_correct = pd.concat(
            objs=[roisfix_dealers_table_correct, roisfix_dealer_table],
            axis=0
        )

    return roisfix_dealers_table_correct


roisfix_dealers_table_correct = correct_roisfix_table(roisfix_dealers_table_t)
roisfix_dealers_table_correct.reset_index(
    drop=True,
    inplace=True
)


def correct_roisfix_rate(roisfix_rate_str):
    try:
        roisfix_rate_str_found = re.search(
            pattern="^\d+[.]\d{2}",
            string=roisfix_rate_str
        ).group()
        return float(roisfix_rate_str_found)
    except:
        return float(0)


roisfix_dealers_table_correct["rate"] = (
    roisfix_dealers_table_correct["rate"].apply(correct_roisfix_rate)
)

roisfix_dealers_table_correct["term_years"] = roisfix_dealers_table_correct["term"].apply(term_years)
roisfix_dealers_table_correct["report_date"] = date.today()
roisfix_dealers_table_correct["rate_name"] = "roisfix"
roisfix_dealers_table_correct.sort_index(
    axis=1,
    inplace=True
)

options = webdriver.ChromeOptions()
options.add_experimental_option('excludeSwitches', ['enable-logging'])
driver = webdriver.Chrome(options=options)

# moscow finance department auctions
# mskfindep_page = driver.get(r"https://www.mos.ru/findep/documents/view/280943220")
# actual_bin = driver.find_element(By.CLASS_NAME, 'department-onedoc ng-scope')
# actual_bin_2 = driver.find_element(By.CLASS_NAME, 'department-onedoc__attach-props')
# download_link = actual_bin_2.get_attribute('href')
#
# mskfindep_pages = read_pdf(
#     input_path=download_link,
#     pages="all",
#     stream=True,
#     encoding="cp1251"
# )
# last_page_num = len(mskfindep_pages) - 1
# mskfindep_table = mskfindep_pages[last_page_num]
# mskfindep_table.rename(
#     columns={
#         "Unnamed: 0": "line_num",
#         "Unnamed: 1": "auction_date",
#         "Unnamed: 2": "deposit_type",
#         "Срок": "term_days",
#         "Сумма": "amount_bn",
#         "Ставка": "rate"
#     },
#     inplace=True
# )
# mskfindep_table["term_days"].fillna(
#     method="ffill",
#     inplace=True
# )
# mskfindep_table.dropna(
#     axis=0,
#     how="any",
#     subset=[
#         "line_num",
#         "auction_date",
#         "deposit_type",
#         "amount_bn",
#         "rate"
#     ],
#     inplace=True
# )
# mskfindep_table.drop(
#     columns=["line_num"],
#     inplace=True
# )
#
#
# def to_date(d):
#     return date(d.year, d.month, d.day)
#
#
# mskfindep_table["auction_date"] = pd.to_datetime(
#     mskfindep_table["auction_date"],
#     format="%d.%m.%y",
# )
#
# mskfindep_table["auction_date"] = mskfindep_table["auction_date"].apply(to_date)
#
#
# def correct_commas(num_with_comma):
#     num_with_dot = num_with_comma.replace(
#         ",",
#         "."
#     )
#     return float(num_with_dot)
#
#
# mskfindep_table["term_days"] = pd.to_numeric(mskfindep_table["term_days"])
# mskfindep_table["term_years"] = mskfindep_table["term_days"] / 365
# mskfindep_table["report_date"] = date.today()
# mskfindep_table["auction_name"] = "mskfindep"
# mskfindep_table.sort_index(
#     axis=1,
#     inplace=True
# )
# mskfindep_table["amount_bn"] = mskfindep_table["amount_bn"].apply(correct_commas)
# mskfindep_table["rate"] = mskfindep_table["rate"].apply(correct_commas)
# mskfindep_table.reset_index(
#     drop=True,
#     inplace=True
# )
mskfindep_table = pd.DataFrame()

# cdb
# cdb_page = driver.get(r"https://www.chinamoney.com.cn/english/bmkycvcyccyccychdt/index.html?bondType=CYCC000&reference=1")
# time.sleep(10)
# cdb_root = driver.page_source
# cdb_table = pd.read_html(cdb_root)[0]


# shibor
shibor_page = driver.get(r"https://www.chinamoney.com.cn/english/bmkshb/")
time.sleep(10)
shibor_root = driver.page_source
shibor_table = pd.read_html(shibor_root)[0]

shibor_report_date = driver.find_element(By.CLASS_NAME, 'text-date').text
shibor_report_date = pd.to_datetime(shibor_report_date, dayfirst=True)

shibor_table.rename(
    columns={
        "Shibor": "term",
        "Rate(%)": "rate"
    },
    inplace=True
)

shibor_table["term_years"] = shibor_table["term"].apply(term_years)
shibor_table["report_date"] = shibor_report_date
shibor_table["rate_name"] = "shibor"
shibor_table.drop(
    columns=['Change(BP)'],
    axis=1,
    inplace=True
)

shibor_table.sort_index(
    axis=1,
    inplace=True
)

# china bonds
china_bonds_page = driver.get(
    r"https://www.chinamoney.com.cn/english/bmkycvcyccyccychdt/index.html?bondType=CYCC000&reference=1")
time.sleep(10)
china_bonds_root = driver.page_source
china_bonds_table = pd.read_html(china_bonds_root)[0]

driver.find_element(By.CLASS_NAME, "page-next").click()
time.sleep(10)
china_bonds_root_2 = driver.page_source
china_bonds_table_2 = pd.read_html(china_bonds_root_2)[0]

china_bonds_table = pd.concat(
    objs=[china_bonds_table, china_bonds_table_2],
    axis=0,
    ignore_index=True,
)

china_bonds_table.rename(
    columns={
        "Date": "report_date",
        "Term": "term_years",
        "Yield to Maturity": "ytm"
    },
    inplace=True
)
china_bonds_table["report_date"] = pd.to_datetime(
    china_bonds_table["report_date"],
    dayfirst=True
)
# china_bonds_table["report_date"] = date.today()
china_bonds_table["rate_name"] = "cfets"
china_bonds_table.sort_index(
    axis=1,
    inplace=True
)

# china IRS
china_irs_page = driver.get(r"https://www.chinamoney.com.cn/english/bmkycvfcc/")
time.sleep(10)
china_irs_root = driver.page_source
china_irs_table = pd.read_html(china_irs_root)[0]

china_irs_table.rename(
    columns={
        "Mean(%)": "rate",
        "Term": "term",
    },
    inplace=True
)

china_irs_table["term_years"] = china_irs_table["term"].apply(term_years)

china_irs_table.drop(
    columns=['Bid(%)', 'Ask(%)'],
    axis=1,
    inplace=True
)

china_irs_table["report_date"] = shibor_report_date
china_irs_table["rate_name"] = "shibor_3m_irs"

china_irs_table.sort_index(
    axis=1,
    inplace=True
)

driver.close()

tod = date.today()
with tempfile.TemporaryDirectory() as tmpdirname:
    file_path_rub_rates = str(tmpdirname) + "\\" + str(tod) + "_rub_rates.xlsx"
    excel_writer_rub_rates = pd.ExcelWriter(path=file_path_rub_rates)
    roisfix_table.to_excel(
        excel_writer=excel_writer_rub_rates,
        sheet_name="roisfix",
        index=False
    )
    roisfix_dealers_table_correct.to_excel(
        excel_writer=excel_writer_rub_rates,
        sheet_name="roisfix_dealers",
        index=False
    )
    mskfindep_table.to_excel(
        excel_writer=excel_writer_rub_rates,
        sheet_name="mskfindep_auctions",
        index=False
    )
    history_rates_table.to_excel(
        excel_writer=excel_writer_rub_rates,
        sheet_name="rusfar_history",
        index=False
    )
    interim_results_rates_table.to_excel(
        excel_writer=excel_writer_rub_rates,
        sheet_name="rusfar_interim",
        index=False
    )

    file_path_cny_rates = str(tmpdirname) + "\\" + str(tod) + "_cny_rates.xlsx"
    excel_writer_cny_rates = pd.ExcelWriter(path=file_path_cny_rates)
    shibor_table.to_excel(
        excel_writer=excel_writer_cny_rates,
        sheet_name="shibor",
        index=False
    )
    china_bonds_table.to_excel(
        excel_writer=excel_writer_cny_rates,
        sheet_name="china_bonds",
        index=False
    )
    china_irs_table.to_excel(
        excel_writer=excel_writer_cny_rates,
        sheet_name="shibor_3m_irs",
        index=False
    )

    file_path_moex_quotes = str(tmpdirname) + "\\" + str(tod) + "_moex_quotes.xlsx"
    excel_writer_moex_quotes = pd.ExcelWriter(path=file_path_moex_quotes)
    history_quotes_table.to_excel(
        excel_writer=excel_writer_moex_quotes,
        sheet_name="history_quotes",
        index=False
    )
    interim_results_table.to_excel(
        excel_writer=excel_writer_moex_quotes,
        sheet_name="interim_results",
        index=False
    )
    zcyc_table.to_excel(
        excel_writer=excel_writer_moex_quotes,
        sheet_name="zcyc",
        index=False
    )

    excel_writer_rub_rates.close()
    excel_writer_cny_rates.close()
    excel_writer_moex_quotes.close()

    # file_path = str(r"C:\Users\nicks\Desktop") + "\\" + str(tod) + "_rates.xlsx"
    # excel_writer = pd.ExcelWriter(path=file_path)
    # roisfix_table.to_excel(
    #     excel_writer=excel_writer,
    #     sheet_name="roisfix",
    #     index=False
    #     )
    # shibor_table.to_excel(
    #     excel_writer=excel_writer,
    #     sheet_name="shibor",
    #     index=False
    #     )
    # excel_writer.close()

    smtpObj = smtplib.SMTP('smtp.gmail.com', 587)
    smtpObj.starttls()
    smtpObj.login('ftpdatamanager@gmail.com', 'cprpcdslmdifsvsz')

    # roisfix_table_text = roisfix_table.to_html(index=False)
    # shibor_table_text = shibor_table.to_html(index=False)

    # date checks
    check_roisfix_table = "none"
    check_roisfix_dealers_table_correct = "none"
    check_mskfindep_table = "none"
    check_shibor_table = "none"
    check_china_bonds_table = "none"
    check_china_irs_table = "none"

    header_1 = f"""
    Report dates: \nroisfix_table {check_roisfix_table}\
        \nroisfix_dealers_table {check_roisfix_dealers_table_correct}\
            \nmskfindep_auctions_table {check_mskfindep_table}\
                \nshibor_table {check_shibor_table}\
                    \nchina_bonds_table {check_china_bonds_table}\
                        \nchina_irs_table {check_china_irs_table}
    """

    subject = f"Shibor, Roisfix, China bonds, Shibor3m IRS update {tod}"
    sender_email = "ftpdatamanager@gmail.com"
    receiver_email = "ftpquuue@mail.ru"
    # polina.varlamova@gazprombank.ru
    # polina.varlamova @ gazprombank.ru
    # , nikita.shuba @ gazprombank.ru

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    # Add body to email
    message.attach(MIMEText(header_1, "plain"))

    filename_1 = file_path_rub_rates  # In same directory as script
    filename_2 = file_path_cny_rates  # In same directory as script
    filename_3 = file_path_moex_quotes  # In same directory as script

    # Open file in binary mode
    with open(filename_1, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    with open(filename_2, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part2 = MIMEBase("application", "octet-stream")
        part2.set_payload(attachment.read())

    with open(filename_3, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part3 = MIMEBase("application", "octet-stream")
        part3.set_payload(attachment.read())

    # Encode file in ASCII characters to send by email
    encoders.encode_base64(part)
    encoders.encode_base64(part2)
    encoders.encode_base64(part3)

    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename_1}",
    )
    part2.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename_2}",
    )
    part3.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename_3}",
    )

    # Add attachment to message and convert message to string
    message.attach(part)
    message.attach(part2)
    message.attach(part3)
    text = message.as_string()

    smtpObj.sendmail(sender_email, receiver_email, text)
    smtpObj.close()