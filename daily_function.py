import pandas as pd
import numpy as np
from datetime import datetime
from config import DefaultValueConfig as dvc, LocalFilePathConfig as lfpc, HankookConfig as hc

user_info = hc.kr_account['account1']
from hankook_api import KISAPIClient
client = KISAPIClient(user_info)


from slack_message import send_simple_message

import FinanceDataReader as fdr

import psutil
import threading
import time
from krxholidays import is_holiday

from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

import os
import traceback


stck_bsop_date = dvc.stck_bsop_date_var
stck_clpr_var = dvc.stck_clpr_var
stck_oprc_var = dvc.stck_oprc_var
stck_hgpr_var = dvc.stck_hgpr_var
stck_lwpr_var = dvc.stck_lwpr_var
lstg_stqt_var = dvc.lstg_stqt_var
pdno_var = dvc.pdno_var

bstp_nmix_hgpr_var = dvc.bstp_nmix_hgpr_var
bstp_nmix_lwpr_var = dvc.bstp_nmix_lwpr_var
bstp_nmix_prpr_var = dvc.bstp_nmix_prpr_var


is_code_uppercase = dvc.is_code_uppercase
is_symbol_uppercase = dvc.is_symbol_uppercase
is_name_uppercase = dvc.is_name_uppercase
is_date_uppercase = dvc.is_date_uppercase
is_total_stock_uppercase = dvc.is_total_stock_uppercase

is_open_uppercase = dvc.is_open_uppercase
is_close_uppercase = dvc.is_close_uppercase
is_high_uppercase = dvc.is_high_uppercase
is_low_uppercase = dvc.is_low_uppercase
is_volume_uppercase = dvc.is_volume_uppercase


date_var = dvc.date_var
name_var = dvc.name_var
symbol_var = dvc.symbol_var
total_stock_var = dvc.total_stock_var
daily_trade_stock_var = dvc.daily_trade_stock_var

close_pr_var = dvc.close_pr_var
open_pr_var = dvc.open_pr_var
high_pr_var = dvc.high_pr_var
low_pr_var = dvc.low_pr_var

type_var = dvc.type_var
cci_ndays = dvc.cci_ndays
cci_index_var = dvc.cci_index_var
open_cci_index_var = dvc.open_cci_index_var
close_cci_index_var = dvc.close_cci_index_var


log_file_path = lfpc.log_file_path
daily_trades_csvs_path = lfpc.daily_trades_csvs_path
daily_best_win_csvs_path = lfpc.daily_best_win_csvs_path
daily_best_return_csvs_path = lfpc.daily_best_return_csvs_path
daily_best_return_per_days_held_csvs_path = lfpc.daily_best_return_per_days_held_csvs_path

daily_progress_final_csvs_path = lfpc.daily_progress_final_csvs_path

# 한국 주식 시장 개장 시간 확인
def is_market_open():
    now = datetime.now()
    
    if is_holiday(now) == False:
        opening_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
        closing_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
        return opening_time <= now <= closing_time
    else:
        print(f'Market is closed due to holiday on {now.strftime("%Y-%m-%d")}')
    return False

# 데이터프레임을 CSV 파일로 저장 또는 업데이트
def update_price_dataframe(symbol, save_path, end_date_str):
    df_csv = pd.read_csv(save_path, parse_dates=[date_var])
    df_csv[date_var] = pd.to_datetime(df_csv[date_var], errors='coerce', format='ISO8601')

    last_date = df_csv[date_var].max()
    if pd.to_datetime(end_date_str) <= last_date:
        # print("No new data to fetch. The latest data is up to date.")
        return df_csv

    else:
        start_date_str = (last_date + pd.Timedelta(days=1)).strftime('%Y%m%d')

        df_new_price = fdr.DataReader(symbol=symbol, start=start_date_str, end=end_date_str).reset_index()
        df_new_price[date_var] = df_new_price[is_date_uppercase]
        df_new_price[open_pr_var] = df_new_price[is_open_uppercase]
        df_new_price[close_pr_var] = df_new_price[is_close_uppercase]
        df_new_price[high_pr_var] = df_new_price[is_high_uppercase]
        df_new_price[low_pr_var] = df_new_price[is_low_uppercase]
        df_new_price[daily_trade_stock_var] = df_new_price[is_volume_uppercase]

        df_new_price = df_new_price[~(df_new_price[open_pr_var] == 0)].reset_index(drop=True)
        
        if len(df_new_price) == 0:
            raise
        else:
            # 기존 데이터프레임과 새 데이터프레임에서 모두-NA 컬럼을 제거
            existing_df_clean = df_csv.dropna(axis=1, how='all')
            df_clean = df_new_price.dropna(axis=1, how='all')
            updated_df = pd.concat([existing_df_clean, df_clean]).drop_duplicates(subset=[date_var]).reset_index(drop=True)
            updated_df[date_var] = pd.to_datetime(updated_df[date_var], errors='coerce', format='ISO8601')
            updated_df = updated_df.drop_duplicates()

    return updated_df

import logging
from functools import wraps

# 디렉터리가 존재하지 않으면 생성
os.makedirs(log_file_path, exist_ok=True)
# 로깅 설정
logging.basicConfig(
    filename=f'{log_file_path}/function_execution.log',  # 로그를 저장할 파일명
    level=logging.INFO,                 # 로깅 레벨 (INFO 이상)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def log_function_call(func):
    """
    데코레이터 함수로, 함수 호출 내역을 로그로 기록합니다.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # 함수 시작 로그
        logging.info(f"Function '{func.__name__}' called with args: {args} and kwargs: {kwargs}")
        result = func(*args, **kwargs)
        # 함수 종료 로그
        logging.info(f"Function '{func.__name__}' finished with result: {result}")
        return result
    return wrapper

@log_function_call
def create_kr_symbol_list(read_dummy : str, save_dummy : str, end_date_str=''):
    """
    Input
    read_dummy(str) : '1' = read csv file, '0' = search online
    save_dummy(str) : '1' = save new csv file, '0' = not save new csv file
    end_date_str(str) : 'YYYYmmdd'
    
    Output
    return_df(DataFrame)
    +---+-------+--------+------------------+-------------+
    |   | type  | symbol |       name       | total_stock |
    +---+-------+--------+------------------+-------------+
    | 0 | stock | 000660 |    SK하이닉스    |  728002365  |
    | 1 | stock | 373220 |  LG에너지솔루션  |  234000000  |
    | 2 | stock | 207940 | 삼성바이오로직스 |  71174000   |
    | 3 | stock | 005380 |      현대차      |  209416191  |
    | 4 | stock | 068270 |     셀트리온     |  217021190  |
    +---+-------+--------+------------------+-------------+
    """

    func_start_datetime = datetime.now()
    func_start_datetiem_str = func_start_datetime.strftime('%Y-%m-%d %H:%M:%S')
    print(f'Start. create_kr_symbol_list, Time: {func_start_datetiem_str}')

    end_date = datetime.now().date()

    if end_date_str != '':
        end_date = datetime.strptime(end_date_str, '%Y%m%d').date()

    # 파일 저장 경로 설정
    file_path = lfpc.daily_kr_symbol_list_from_hankook_path  # 실제 저장 경로
    os.makedirs(file_path, exist_ok=True)

    strf_date = end_date.strftime("%Y%m%d")
    file_name = f'{strf_date}_hankook_kospi_symbol.csv'
    save_path = os.path.join(file_path, file_name)


    def create_new_kr_stock_symbol_list(save_path, save_dummy):
        
        kr_stock_df = fdr.StockListing('KRX')
        kr_stock_df[type_var] = 'stock'
        kr_stock_df[name_var] = kr_stock_df[is_name_uppercase]
        kr_stock_df[symbol_var] = kr_stock_df[is_code_uppercase] # fdr.StockListing('KRX')  return Code
        kr_stock_df[total_stock_var] = kr_stock_df[is_total_stock_uppercase].astype(float).astype('int64')

        krx_administrative_df = fdr.StockListing('KRX-ADMINISTRATIVE')
        krx_administrative_df[symbol_var] = krx_administrative_df[is_symbol_uppercase]
        krx_administrative_symbol_list = krx_administrative_df[symbol_var].unique().tolist()
        kr_stock_df = kr_stock_df[~kr_stock_df[symbol_var].isin(krx_administrative_symbol_list)].reset_index(drop=True)

        kr_etf_df = fdr.StockListing('ETF/KR')
        kr_etf_df[type_var] = 'etf'
        kr_etf_df[name_var] = kr_etf_df[is_name_uppercase]
        kr_etf_df[symbol_var] = kr_etf_df[is_symbol_uppercase]
        
        kr_etf_df[total_stock_var] = 0

        for etf_symbol in kr_etf_df[symbol_var].unique().tolist():

            try:
                t_df = client.search_stock_info(PDNO=str(etf_symbol))
                t_total_stock = int(float(t_df[lstg_stqt_var].iloc[0]))
                kr_etf_df[total_stock_var] = np.where(kr_etf_df[symbol_var] == etf_symbol, t_total_stock, kr_etf_df[total_stock_var])
            except:
                pass

        krx_symbol_list_df = pd.concat([kr_stock_df, kr_etf_df])
        krx_symbol_list_df[date_var] = end_date
        krx_symbol_list_df[total_stock_var] = krx_symbol_list_df[total_stock_var].astype(float).astype('int64')
        krx_symbol_list_df = krx_symbol_list_df[krx_symbol_list_df[total_stock_var]>0].reset_index(drop=True)

        new_df = krx_symbol_list_df[[type_var, symbol_var, name_var, total_stock_var]].reset_index(drop=True)

        if save_dummy == '1':
            os.makedirs(file_path, exist_ok=True)
            if not new_df.empty:
                new_df.to_csv(save_path, index=False, encoding='utf-8-sig')
            else:
                print(f"Warning: Attempted to save an empty DataFrame to {save_path}.")

        return new_df


    if read_dummy == '1':
        if os.path.exists(save_path):
            return_df = pd.read_csv(save_path, dtype={symbol_var:str})
        else:
            return_df = create_new_kr_stock_symbol_list(save_path=save_path, save_dummy=save_dummy)
    else:
        return_df = create_new_kr_stock_symbol_list(save_path=save_path, save_dummy=save_dummy)


    func_end_datetime = datetime.now()
    func_end_date_str = func_end_datetime.strftime('%Y-%m-%d %H:%M:%S')
    func_run_diff_seconds = int(round((func_end_datetime - func_start_datetime).total_seconds(),0))
    print(f'End. create_kr_symbol_list, Time: {func_end_date_str}, running seconds:{func_run_diff_seconds}')

    return return_df

def load_min_date_from_csv():
    file_path = lfpc.min_price_date_csv_path 
    file_name = 'kr_stock_price_min_date.csv'
    min_date_file = os.path.join(file_path, file_name)
    if os.path.exists(min_date_file):
        try:
            df = pd.read_csv(min_date_file, index_col=symbol_var)
            if not df.empty:
                df['min_date'] = pd.to_datetime(df['min_date'], errors='coerce', format='ISO8601')
            return df
        except pd.errors.EmptyDataError:
            return pd.DataFrame(columns=[symbol_var, 'min_date']).set_index(symbol_var)

    else:
        return pd.DataFrame(columns=[symbol_var, 'min_date']).set_index(symbol_var)

def save_min_date(symbol: str, min_date):
    file_path = lfpc.min_price_date_csv_path 
    file_name = 'kr_stock_price_min_date.csv'
    os.makedirs(file_path, exist_ok=True)
    min_date_file = os.path.join(file_path, file_name)

    df_min_dates = load_min_date_from_csv()

    df_min_dates.loc[symbol] = pd.to_datetime(min_date, errors='coerce', format='ISO8601')

    df_min_dates.to_csv(min_date_file)

def get_min_date(symbol: str):
    df_min_dates = load_min_date_from_csv()
    if symbol in df_min_dates.index:
        return df_min_dates.loc[symbol]['min_date']
    else:
        min_date = fdr.DataReader(symbol=symbol).index.min()
        save_min_date(symbol, min_date)
        return min_date

# 주가 데이터 조회
def update_daily_stock_price(symbol, read_dummy, save_dummy, end_date_str='', start_date_str='19000101'):
    func_start_datetime = datetime.now()
    func_start_datetiem_str = func_start_datetime.strftime('%Y-%m-%d %H:%M:%S')
    # print(f'Start. update_daily_stock_price, symbol : {symbol}, Time: {func_start_datetiem_str}')

    """
    Input:
    symbol (str): Stock symbol for which the daily price is to be updated.
    read_dummy (str): If '1', use existing data if available; otherwise, fetch new data.
    save_dummy (str): If '1', save the resulting DataFrame as a CSV file.
    start_date_str (str): Start date in 'YYYYmmdd' format. Default is '19000101'.
    end_date_str (str): End date in 'YYYYmmdd' format. Default is '' (latest date).

    Output:
    Returns a single DataFrame containing the updated daily stock price information.
    """

    start_date = pd.to_datetime(start_date_str, errors='coerce', format='ISO8601')
    search_start_date = start_date
    
    min_date = get_min_date(symbol)
    min_date = pd.to_datetime(min_date, errors='coerce', format='ISO8601')

    if start_date_str == '19000101':
        search_start_date = min_date

    if start_date < min_date:
        search_start_date = min_date

    if start_date >= min_date:
        search_start_date = start_date

    today_date = datetime.now().date()
    today_date = pd.to_datetime(today_date, errors='coerce', format='ISO8601')
    search_end_date = today_date
    
    if end_date_str != '':
        end_date = datetime.strptime(end_date_str, '%Y%m%d')
        if end_date >= today_date:
            search_end_date = today_date
        if end_date < today_date:
            search_end_date = end_date

    search_start_date_str = search_start_date.strftime('%Y%m%d')
    search_end_date_str = search_end_date.strftime('%Y%m%d')

    file_path = lfpc.daily_price_csvs_path
    os.makedirs(file_path, exist_ok=True)

    file_name = f'kr_symbol_{symbol}.csv'
    save_path = f"{file_path}/{file_name}"

    # index_symbol_list = ['kospi', 'kosdaq']
    
    df_update_min_date_data = None
    df_update_max_date_data = None

    if read_dummy == '0':
        df_new_price = fdr.DataReader(symbol=symbol, start=search_start_date_str, end=search_end_date_str).reset_index()
        if df_new_price.empty:
            print(f"No data found for symbol {symbol} from {search_start_date_str} to {search_end_date_str}.")
            return pd.DataFrame()
        
        df_new_price[date_var] = df_new_price[is_date_uppercase]
        df_new_price[open_pr_var] = df_new_price[is_open_uppercase]
        df_new_price[close_pr_var] = df_new_price[is_close_uppercase]
        df_new_price[high_pr_var] = df_new_price[is_high_uppercase]
        df_new_price[low_pr_var] = df_new_price[is_low_uppercase]
        df_new_price[daily_trade_stock_var] = df_new_price[is_volume_uppercase]
        df_new_price[date_var] = pd.to_datetime(df_new_price[date_var], errors='coerce', format='ISO8601')

        df_new_price = df_new_price[~(df_new_price[open_pr_var] == 0)].reset_index(drop=True)

        if save_dummy == '1':
            raise Exception("Cannot save when read_dummy is 0.")
        

        func_end_datetime = datetime.now()
        func_end_date_str = func_end_datetime.strftime('%Y-%m-%d %H:%M:%S')
        func_run_diff_seconds = int(round((func_end_datetime - func_start_datetime).total_seconds(),0))
        # print(f'End. update_daily_stock_price, symbol : {symbol}, Time: {func_end_date_str}, running seconds:{func_run_diff_seconds}')
        return df_new_price

    if read_dummy == '1':
        # Check if the file exists
        if os.path.exists(save_path):
            df_csv = pd.read_csv(save_path)
            if not df_csv.empty:
                df_csv[date_var] = pd.to_datetime(df_csv[date_var], errors='coerce', format='ISO8601')
                csv_min_date = df_csv[date_var].min()
                csv_max_date = df_csv[date_var].max()
            elif df_csv.empty:
                df_new_price = fdr.DataReader(symbol=symbol, start=search_start_date_str, end=search_end_date_str).reset_index()
                if df_new_price.empty:
                    print(f"No data found for symbol {symbol} from {search_start_date_str} to {search_end_date_str}.")
                    return pd.DataFrame()
                if not df_new_price.empty:
                    df_new_price[date_var] = df_new_price[is_date_uppercase]
                    df_new_price[open_pr_var] = df_new_price[is_open_uppercase]
                    df_new_price[close_pr_var] = df_new_price[is_close_uppercase]
                    df_new_price[high_pr_var] = df_new_price[is_high_uppercase]
                    df_new_price[low_pr_var] = df_new_price[is_low_uppercase]
                    df_new_price[daily_trade_stock_var] = df_new_price[is_volume_uppercase]
                    df_new_price[date_var] = pd.to_datetime(df_new_price[date_var], errors='coerce', format='ISO8601')
                    
                    df_new_price = df_new_price[~(df_new_price[open_pr_var] == 0)].reset_index(drop=True)

                    if save_dummy == '1':
                        if not df_new_price.empty:
                            df_new_price.to_csv(save_path, index=False, encoding='utf-8-sig')  # 업데이트된 데이터 저장
                            return df_new_price
                else:
                    return pd.DataFrame()

            if csv_min_date <= search_start_date and csv_max_date >= search_end_date:
                return_df = df_csv[(df_csv[date_var]>=search_start_date) & (df_csv[date_var]<=search_end_date)].reset_index(drop=True)
                func_end_datetime = datetime.now()
                func_end_date_str = func_end_datetime.strftime('%Y-%m-%d %H:%M:%S')
                func_run_diff_seconds = int(round((func_end_datetime - func_start_datetime).total_seconds(),0))
                # print(f'End. update_daily_stock_price, symbol : {symbol}, Time: {func_end_date_str}, running seconds:{func_run_diff_seconds}')
                return return_df

            if csv_min_date >= search_start_date:
                df_new_price = fdr.DataReader(symbol=symbol, start=search_start_date_str, end=search_end_date_str).reset_index()
                if df_new_price.empty:
                    print(f"No data found for symbol {symbol} from {search_start_date_str} to {search_end_date_str}.")
                    return pd.DataFrame()

                df_new_price[date_var] = df_new_price[is_date_uppercase]
                df_new_price[open_pr_var] = df_new_price[is_open_uppercase]
                df_new_price[close_pr_var] = df_new_price[is_close_uppercase]
                df_new_price[high_pr_var] = df_new_price[is_high_uppercase]
                df_new_price[low_pr_var] = df_new_price[is_low_uppercase]
                df_new_price[daily_trade_stock_var] = df_new_price[is_volume_uppercase]

                df_new_price[date_var] = pd.to_datetime(df_new_price[date_var], errors='coerce', format='ISO8601')
                df_new_price = df_new_price[~(df_new_price[open_pr_var] == 0)].reset_index(drop=True)

                df_update_min_date_data = pd.concat([df_csv, df_new_price])
                df_update_min_date_data[date_var] = pd.to_datetime(df_update_min_date_data[date_var], errors='coerce', format='ISO8601')
                df_update_min_date_data = df_update_min_date_data.drop_duplicates(subset=[date_var], keep='last')

            if csv_max_date <= search_end_date:
                df_new_price = fdr.DataReader(symbol=symbol, start=search_start_date_str, end=search_end_date_str).reset_index()
                if df_new_price.empty:
                    print(f"No data found for symbol {symbol} from {search_start_date_str} to {search_end_date_str}.")
                    return pd.DataFrame()

                df_new_price[date_var] = df_new_price[is_date_uppercase]
                df_new_price[open_pr_var] = df_new_price[is_open_uppercase]
                df_new_price[close_pr_var] = df_new_price[is_close_uppercase]
                df_new_price[high_pr_var] = df_new_price[is_high_uppercase]
                df_new_price[low_pr_var] = df_new_price[is_low_uppercase]
                df_new_price[daily_trade_stock_var] = df_new_price[is_volume_uppercase]


                df_new_price[date_var] = pd.to_datetime(df_new_price[date_var], errors='coerce', format='ISO8601')
                df_new_price = df_new_price[~(df_new_price[open_pr_var] == 0)].reset_index(drop=True)

                df_update_max_date_data = pd.concat([df_csv, df_new_price])
                df_update_max_date_data[date_var] = pd.to_datetime(df_update_max_date_data[date_var], errors='coerce', format='ISO8601')
                df_update_max_date_data = df_update_max_date_data.drop_duplicates(subset=[date_var], keep='last')

            df_updated = pd.concat([df_update_min_date_data, df_update_max_date_data])
            df_updated = df_updated.drop_duplicates(subset=[date_var], keep='last')
            
            if save_dummy == '1':
                if not df_updated.empty:
                    df_updated.to_csv(save_path, index=False, encoding='utf-8-sig')  # 업데이트된 데이터 저장
                else:
                    print(f"Warning: Attempted to save an empty DataFrame to {save_path}.")

            func_end_datetime = datetime.now()
            func_end_date_str = func_end_datetime.strftime('%Y-%m-%d %H:%M:%S')
            func_run_diff_seconds = int(round((func_end_datetime - func_start_datetime).total_seconds(),0))
            # print(f'End. update_daily_stock_price, symbol : {symbol}, Time: {func_end_date_str}, running seconds:{func_run_diff_seconds}')
            return df_updated

        else:
            # warnings.warn(f"The requested file does not exist in the {save_path}, so the data for the currently selected period has been saved.")
            df_new_price = fdr.DataReader(symbol=symbol, start=start_date_str, end=end_date_str).reset_index()
            

            df_new_price[date_var] = df_new_price[is_date_uppercase]
            df_new_price[open_pr_var] = df_new_price[is_open_uppercase]
            df_new_price[close_pr_var] = df_new_price[is_close_uppercase]
            df_new_price[high_pr_var] = df_new_price[is_high_uppercase]
            df_new_price[low_pr_var] = df_new_price[is_low_uppercase]
            df_new_price[daily_trade_stock_var] = df_new_price[is_volume_uppercase]


            df_new_price[date_var] = pd.to_datetime(df_new_price[date_var], errors='coerce', format='ISO8601')
            df_new_price = df_new_price[~(df_new_price[open_pr_var] == 0)].reset_index(drop=True)
            
            # 시간을 명시적으로 00:00:00으로 추가
            df_new_price[date_var] = df_new_price[date_var].apply(lambda x: x if x.time() != pd.Timestamp.min.time() else pd.Timestamp(x.date()))
            
            if save_dummy == '1':
                if not df_new_price.empty:
                    df_new_price.to_csv(save_path, index=False, encoding='utf-8-sig')
                else:
                    print(f"Warning: Attempted to save an empty DataFrame to {save_path}.")
            func_end_datetime = datetime.now()
            func_end_date_str = func_end_datetime.strftime('%Y-%m-%d %H:%M:%S')
            func_run_diff_seconds = int(round((func_end_datetime - func_start_datetime).total_seconds(),0))
            # print(f'End. update_daily_stock_price, Time: {func_end_date_str}, running seconds:{func_run_diff_seconds}')
            return df_new_price
    

def update_cci_data(symbol, read_dummy, save_dummy, end_date_str):
    file_path = lfpc.daily_cci_index_csvs_path
    file_name = f'kr_cci_symbol_{symbol}.csv'
    save_path = os.path.join(file_path, file_name)

    # Ensure the directory exists
    os.makedirs(file_path, exist_ok=True)

    def create_new_cci_data(df_price):
        # Ensure required columns are numeric
        numeric_columns = [high_pr_var, low_pr_var, open_pr_var, close_pr_var, daily_trade_stock_var]
        for col in numeric_columns:
            if col in df_price.columns:
                df_price[col] = pd.to_numeric(df_price[col], errors='coerce')
        
        # Calculate Typical Price (TP)
        df_price["close_TP"] = (df_price[high_pr_var] + df_price[low_pr_var] + df_price[close_pr_var]) / 3
        df_price["open_TP"] = (df_price[high_pr_var] + df_price[low_pr_var] + df_price[open_pr_var]) / 3

        # Calculate Simple Moving Average (SMA) and Mean Absolute Deviation (MAD)
        df_price["close_sma"] = df_price["close_TP"].rolling(cci_ndays).mean()
        df_price["open_sma"] = df_price["open_TP"].rolling(cci_ndays).mean()
        df_price["close_mad"] = df_price["close_TP"].rolling(cci_ndays).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
        df_price["open_mad"] = df_price["open_TP"].rolling(cci_ndays).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)

        # Calculate Commodity Channel Index (CCI)
        df_price[close_cci_index_var] = (df_price["close_TP"] - df_price["close_sma"]) / (0.015 * df_price["close_mad"])
        df_price[open_cci_index_var] = (df_price["open_TP"] - df_price["open_sma"]) / (0.015 * df_price["open_mad"])

        return df_price

    df_cci = pd.DataFrame()
    df_new_cci = pd.DataFrame()

    df_price = update_daily_stock_price(symbol=symbol, read_dummy=read_dummy, save_dummy=save_dummy, end_date_str=end_date_str)
    if df_price.empty:
        print(f"Error: No price data available for symbol {symbol}. Skipping CCI calculation.")
        return pd.DataFrame()
    else:
        df_price[date_var] = pd.to_datetime(df_price[date_var], errors='coerce')

        # Handle empty or missing CCI file
        if read_dummy == '1':
            if os.path.exists(save_path):
                if os.path.getsize(save_path) > 0:
                    try:
                        df_cci = pd.read_csv(save_path)
                        if len(df_cci) > 0:
                            df_cci[date_var] = pd.to_datetime(df_cci[date_var], errors='coerce')
                        else:
                            pass

                    except pd.errors.EmptyDataError:
                        pass

                else:
                    pass
            else:
                pass

        if read_dummy == '0':
            df_cci = create_new_cci_data(df_price)
            return df_cci

        # Calculate new CCI data if needed
        if not df_cci.empty:
            cci_max_date = df_cci[date_var].max()
            price_max_date = df_price[date_var].max()

            if price_max_date > cci_max_date:
                df_new_cci = create_new_cci_data(df_price)
                df_combined = pd.concat([df_cci, df_new_cci]).drop_duplicates(subset=[date_var], keep='last')
                if save_dummy == '1':
                    df_combined.to_csv(save_path, index=False, encoding='utf-8-sig')
                return df_combined
            else:
                # print(f"No new CCI data to update for symbol {symbol}.")
                return df_cci
        else:
            df_new_cci = create_new_cci_data(df_price)
            if save_dummy == '1':
                df_new_cci.to_csv(save_path, index=False, encoding='utf-8-sig')
            return df_new_cci


# 데이터에서 날짜 컬럼을 확인하고 날짜 형식으로 변환
def ensure_datetime_format(df, date_column=date_var):
    if date_column in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df[date_column]):
            df[date_column] = pd.to_datetime(df[date_column])
    return df

# 매매 데이터 업데이트 및 필터링
def get_filtered_cci_data(symbol, read_dummy, save_dummy, end_date_str, search_history_year):
    
    df_cci = update_cci_data(symbol=symbol, read_dummy=read_dummy, save_dummy=save_dummy, end_date_str=end_date_str)
    if 'index' in df_cci.columns:
        df_cci = df_cci.drop(columns=['index'])
    df_cci = ensure_datetime_format(df_cci)

    if search_history_year == 'all':
        return df_cci
    else:
        one_year_ago = df_cci[date_var].max() - pd.DateOffset(years=int(search_history_year))
        df_filtered_cci = df_cci[df_cci[date_var] >= one_year_ago].reset_index(drop=True)
        return df_filtered_cci



def create_trade_history_by_symbol(
    symbol, holding_days, target_return_values, search_history_years,
    buy_cci_thresholds, stop_loss_cci_thresholds, read_dummy, save_dummy, df_kr, end_date_str
):
    """
    Generate trade history for a specific symbol with updated Pandas methods.

    Returns:
    - df_all_trades: DataFrame with all trade records.
    - stock_name: Name of the stock.
    - stock_type: Type of the stock.
    - last_price_date: Latest price date in the dataset.
    """
    print(f"Processing trade history for symbol: {symbol}")

    # 결과를 저장할 리스트 초기화
    all_trades = []

    for search_year in search_history_years:
        df = get_filtered_cci_data(symbol, read_dummy, save_dummy, end_date_str, search_year)
        if df.empty:
            print(f"No data available for symbol: {symbol}, year: {search_year}. Skipping.")
            continue

        df = df.drop_duplicates(subset=[date_var]).reset_index(drop=True)
        last_price_date = df[date_var].max()

        for x in holding_days:
            for target_return_percent in target_return_values:
                for buy_cci_threshold in buy_cci_thresholds:
                    for stop_loss_cci_threshold in stop_loss_cci_thresholds:
                        # 매수 조건을 벡터화
                        buy_signals = (df[open_cci_index_var].shift(1) < buy_cci_threshold) & \
                                      (df[open_cci_index_var] > buy_cci_threshold)

                        # 매수일 선택
                        buy_indices = df.index[buy_signals]
                        if buy_indices.empty:
                            continue

                        # 매수일 이후 데이터 추출
                        for buy_idx in buy_indices:
                            # 매수 조건
                            buy_price = df.loc[buy_idx, open_pr_var]
                            buy_date = df.loc[buy_idx, date_var]

                            # 매수 후 기간 데이터
                            future_data = df.iloc[buy_idx + 1:buy_idx + 1 + x]

                            if future_data.empty:
                                continue

                            # 목표가 및 손절가 조건
                            target_prices = buy_price * (1 + target_return_percent / 100)
                            stop_loss_condition = future_data[close_cci_index_var] <= stop_loss_cci_threshold
                            target_condition = future_data[close_pr_var] >= target_prices

                            # 목표가 달성일 및 손절일
                            target_reach_idx = future_data.index[target_condition].min() if target_condition.any() else None
                            stop_loss_idx = future_data.index[stop_loss_condition].min() if stop_loss_condition.any() else None

                            # 종료 조건 판단
                            if target_reach_idx is not None and (stop_loss_idx is None or target_reach_idx < stop_loss_idx):
                                reach_target_date = future_data.loc[target_reach_idx, date_var]
                                reach_target_price = future_data.loc[target_reach_idx, close_pr_var]
                                all_trades.append({
                                    'condition_history_search_year': search_year,
                                    'buy_date': buy_date,
                                    'buy_price': buy_price,
                                    'reach_target_date': reach_target_date,
                                    'reach_target_price': reach_target_price,
                                    'stop_loss_date': None,
                                    'stop_loss_price': None,
                                    'maturity_date': None,
                                    'maturity_price': None,
                                    'days_held': (reach_target_date - buy_date).days,
                                    'condition_holding_days': x,
                                    'condition_target_return': target_return_percent,
                                    'condition_buy_cci_threshold': buy_cci_threshold,
                                    'condition_stop_loss_cci_threshold': stop_loss_cci_threshold
                                })
                            elif stop_loss_idx is not None:
                                stop_loss_date = future_data.loc[stop_loss_idx, date_var]
                                stop_loss_price = future_data.loc[stop_loss_idx, close_pr_var]
                                all_trades.append({
                                    'condition_history_search_year': search_year,
                                    'buy_date': buy_date,
                                    'buy_price': buy_price,
                                    'reach_target_date': None,
                                    'reach_target_price': None,
                                    'stop_loss_date': stop_loss_date,
                                    'stop_loss_price': stop_loss_price,
                                    'maturity_date': None,
                                    'maturity_price': None,
                                    'days_held': (stop_loss_date - buy_date).days,
                                    'condition_holding_days': x,
                                    'condition_target_return': target_return_percent,
                                    'condition_buy_cci_threshold': buy_cci_threshold,
                                    'condition_stop_loss_cci_threshold': stop_loss_cci_threshold
                                })
                            else:
                                maturity_date = future_data.iloc[-1][date_var]
                                maturity_price = future_data.iloc[-1][close_pr_var]
                                all_trades.append({
                                    'condition_history_search_year': search_year,
                                    'buy_date': buy_date,
                                    'buy_price': buy_price,
                                    'reach_target_date': None,
                                    'reach_target_price': None,
                                    'stop_loss_date': None,
                                    'stop_loss_price': None,
                                    'maturity_date': maturity_date,
                                    'maturity_price': maturity_price,
                                    'days_held': x,
                                    'condition_holding_days': x,
                                    'condition_target_return': target_return_percent,
                                    'condition_buy_cci_threshold': buy_cci_threshold,
                                    'condition_stop_loss_cci_threshold': stop_loss_cci_threshold
                                })

    # 리스트를 DataFrame으로 변환
    df_all_trades = pd.DataFrame(all_trades)

    # 종목 정보 가져오기
    stock_info = df_kr[df_kr[symbol_var] == symbol]
    stock_name = stock_info[name_var].iloc[0] if not stock_info.empty else None
    stock_type = stock_info[type_var].iloc[0] if not stock_info.empty else None

    # 결과 저장 및 반환
    trades_save_path = os.path.join(daily_trades_csvs_path, f"all_trades_{symbol}_end_date_{end_date_str}.csv")

    # 같은 symbol의 과거 end_date_str 파일 삭제
    for file_name in os.listdir(daily_trades_csvs_path):
        if file_name.startswith(f"all_trades_{symbol}_end_date_") and file_name != f"all_trades_{symbol}_end_date_{end_date_str}.csv":
            file_path = os.path.join(daily_trades_csvs_path, file_name)
            print(f"Deleting old file: {file_path}")
            os.remove(file_path)

    # 새로운 결과 저장
    df_all_trades.to_csv(trades_save_path, index=False, encoding='utf-8-sig')

    return df_all_trades, stock_name, stock_type, last_price_date


# 하드웨어 자원 모니터링 함수
def monitor_resources(stop_event, interval=600):
    """
    Monitor system resources and stop when stop_event is set.
    """
    print("Resource monitoring started.")
    try:
        while not stop_event.is_set():
            cpu_usage = psutil.cpu_percent(interval=1)
            memory_info = psutil.virtual_memory()
            memory_usage = memory_info.percent
            print(f"CPU Usage: {cpu_usage}%, Memory Usage: {memory_usage}%")
            time.sleep(interval)
    except Exception as e:
        print(f"Error during monitoring: {e}")
    finally:
        print("Resource monitoring stopped.")

# 병렬로 함수 실행 시 모니터링을 별도의 스레드로 구동
def start_monitoring_in_background():
    """
    Start monitoring resources in a separate thread and return a stop event.
    """
    try:
        stop_event = threading.Event()
        monitor_thread = threading.Thread(target=monitor_resources, args=(stop_event,), daemon=True)
        monitor_thread.start()
        return stop_event
    except Exception as e:
        print(f"Error starting monitoring: {e}")
        traceback.print_exc()
        return None  # Ensure this does not happen in the main logic


def stop_monitoring(stop_event):
    """
    Stop monitoring by setting the stop_event.
    """
    if stop_event is None:
        print("Error: stop_event is None. Cannot stop monitoring.")
        return
    if stop_event.is_set():
        print("Monitoring already stopped.")
    else:
        print("Stopping monitoring process...")
        stop_event.set()  # Set the event to stop monitoring
        print("Monitoring successfully stopped.")

def process_symbol(symbol, holding_days, target_return_values, search_history_years,
            buy_cci_thresholds, stop_loss_cci_thresholds, read_dummy, save_dummy, df_kr, end_date_str):
    try:
        # 매매 기록 생성
        result = create_trade_history_by_symbol(
            symbol, holding_days, target_return_values, search_history_years,
            buy_cci_thresholds, stop_loss_cci_thresholds, read_dummy, save_dummy, df_kr, end_date_str
        )
        if not result or result[0].empty:
            return None, symbol  # 매매 기록이 없으면 스킵
        return result, symbol

    except Exception as e:
        print(f"Error processing symbol {symbol}: {e}")
        traceback.print_exc()
        return None, symbol

@log_function_call
def process_all_stocks_with_save_optimized(
    df_kr, holding_days, target_return_values, search_history_years, buy_cci_thresholds,
    stop_loss_cci_thresholds, read_dummy, save_dummy, end_date_str
):

    func_start_datetime = datetime.now()
    print(f'Start. process_all_stocks_with_save_optimized, Time: {func_start_datetime.strftime("%Y-%m-%d %H:%M:%S")}')

    # Start resource monitoring
    stop_event = start_monitoring_in_background()
    if stop_event is None:
        raise RuntimeError("Failed to start monitoring. 'stop_event' is None.")

    try:

        stock_symbols = df_kr[df_kr[type_var]=='stock'].loc[:199][symbol_var].tolist()
        etf_symbols = df_kr[df_kr[type_var]=='etf'].reset_index(drop=True).loc[:199][symbol_var].tolist()

        all_symbols = stock_symbols + etf_symbols
        processed_symbols = []

        # 진행 상태 저장 경로
        os.makedirs(daily_progress_final_csvs_path, exist_ok=True)
        progress_file_path = os.path.join(daily_progress_final_csvs_path, f'progress_{end_date_str}.csv')

        # 진행 상태 로드
        if os.path.exists(progress_file_path):
            processed_symbols = pd.read_csv(progress_file_path, dtype=str)[symbol_var].tolist()

        done_count = len(processed_symbols)
        remaining_symbols = [sym for sym in all_symbols if sym not in processed_symbols]
        print(f"Total symbols: {len(all_symbols)}, Remaining symbols: {len(remaining_symbols)}")

        # 최적 조건 결과 파일 경로
        win_save_path = os.path.join(daily_best_win_csvs_path, f'final_best_win_{end_date_str}.csv')
        return_save_path = os.path.join(daily_best_return_csvs_path, f'final_best_return_{end_date_str}.csv')
        return_per_days_held_save_path = os.path.join(daily_best_return_per_days_held_csvs_path, f'final_best_return_per_days_held_{end_date_str}.csv')

        os.makedirs(daily_best_win_csvs_path, exist_ok=True)
        os.makedirs(daily_best_return_csvs_path, exist_ok=True)
        os.makedirs(daily_best_return_per_days_held_csvs_path, exist_ok=True)


        # 기존 데이터 로드
        if os.path.exists(win_save_path):
            final_best_win_df = pd.read_csv(win_save_path, dtype={symbol_var:str})
        else:
            final_best_win_df = pd.DataFrame()

        if os.path.exists(return_save_path):
            final_best_return_df = pd.read_csv(return_save_path, dtype={symbol_var:str})
        else:
            final_best_return_df = pd.DataFrame()

        if os.path.exists(return_per_days_held_save_path):
            final_best_return_per_days_held_df = pd.read_csv(return_per_days_held_save_path, dtype={symbol_var:str})
        else:
            final_best_return_per_days_held_df = pd.DataFrame()

        # 병렬 처리
        num_workers = min(len(remaining_symbols), multiprocessing.cpu_count() - 1)
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            futures = {
                executor.submit(
                process_symbol,
                sym,  # 심볼
                holding_days,  # 보유 기간
                target_return_values,  # 목표 수익률
                search_history_years,  # 검색할 연도
                buy_cci_thresholds,  # CCI 구매 임계값
                stop_loss_cci_thresholds,  # 손절 임계값
                read_dummy,  # 읽기 설정
                save_dummy,  # 저장 설정
                df_kr,  # 한국 주식 데이터프레임
                end_date_str  # 종료 날짜
                ): sym for sym in remaining_symbols
                }

            for future in as_completed(futures):
                try:
                    result, symbol = future.result()
                    if result:
                        df_all_trades, stock_name, stock_type, last_price_date = result
                        # df_all_trades가 빈 DataFrame인지 확인
                        if df_all_trades.empty:
                            print(f"df_all_trades is empty for symbol: {symbol}. Skipping further processing.")

                            # 진행 상태 업데이트만 수행
                            processed_symbols.append(symbol)
                            pd.DataFrame({symbol_var: processed_symbols}, dtype=str).to_csv(progress_file_path, index=False, encoding='utf-8-sig')
                            done_count += 1
                            print(f"Processed and passed symbol: {symbol}, {done_count}/{len(all_symbols)}")
                            continue  # 다음 심볼로 넘어감
                        try:
                            # 날짜 열 통일
                            date_columns = ['reach_target_date', 'stop_loss_date', 'maturity_date']
                            for col in date_columns:
                                df_all_trades[col] = pd.to_datetime(df_all_trades[col], errors='coerce')

                            # 가격 열 통일
                            price_columns = ['reach_target_price', 'stop_loss_price', 'maturity_price']
                            for col in price_columns:
                                df_all_trades[col] = pd.to_numeric(df_all_trades[col], errors='coerce')

                            # 이후 .bfill() 호출
                            df_all_trades['sell_date'] = pd.to_datetime(
                                df_all_trades[date_columns].bfill(axis=1).iloc[:, 0],
                                errors='coerce'
                            )

                            df_all_trades['sell_price'] = pd.to_numeric(
                                df_all_trades[price_columns].bfill(axis=1).iloc[:, 0],
                                errors='coerce'
                            )

                            df_all_trades['sell_price'] = pd.to_numeric(df_all_trades['sell_price'], errors='coerce')
                            df_all_trades['reach_target_amount'] = df_all_trades['sell_price'] - df_all_trades['buy_price']
                            df_all_trades['reach_target_amount_per_days_held'] = round(df_all_trades['reach_target_amount'] / df_all_trades['days_held'], 2)

                            df_all_trades['win_dummy'] = 0
                            df_all_trades['win_dummy'] = np.where(df_all_trades['sell_price'] > df_all_trades['buy_price'], 1, df_all_trades['win_dummy'])

                            df_all_trades['lose_dummy'] = 0
                            df_all_trades['lose_dummy'] = np.where(df_all_trades['sell_price'] < df_all_trades['buy_price'], 1, df_all_trades['lose_dummy'])

                            grouped_cols = ['condition_holding_days', 'condition_target_return', 'condition_buy_cci_threshold', 'condition_stop_loss_cci_threshold']

                            for sy in search_history_years:
                                df_r1 = df_all_trades[df_all_trades['condition_history_search_year'] == sy].reset_index(drop=True)
                                if df_r1.empty:
                                    print(f"search_year_{sy}_trades_empty: {symbol}. Skipping further processing.")
                                    continue

                                df_results = df_r1.groupby(grouped_cols).agg(
                                    count_buy_date = ('buy_date', 'nunique'),
                                    count_reach_target_date = ('reach_target_date', 'nunique'),
                                    count_stop_loss_date = ('stop_loss_date', 'nunique'),
                                    count_maturity_date = ('maturity_date', 'nunique'),
                                    count_win = ('win_dummy', 'sum'),
                                    count_lose = ('lose_dummy', 'sum'),
                                    avg_revenue_per_days_held= ('reach_target_amount_per_days_held', 'mean'),
                                    avg_days_held = ('days_held', 'mean'),
                                    sum_buy_price = ('buy_price', 'sum'),
                                    sum_reach_target_price = ('reach_target_price', 'sum'),
                                    sum_stop_loss_price = ('stop_loss_price', 'sum'),
                                    sum_maturity_price = ('maturity_price', 'sum'),
                                    sum_sell_price = ('sell_price', 'sum')
                                    ).reset_index()

                                df_results['win_rate'] = round(df_results['count_win'] / df_results['count_buy_date'] * 100, 2)
                                df_results['lose_rate'] = round(df_results['count_lose'] / df_results['count_buy_date'] * 100, 2)

                                df_results['revenue_rate'] = round(((df_results['sum_sell_price']-df_results['sum_buy_price']) / df_results['sum_buy_price'])*100, 2)
                                df_results['reach_target_date_count_per_buy_date_count'] = round(df_results['count_reach_target_date'] / df_results['count_buy_date']*100, 2)
                                df_results['stop_loss_date_count_per_buy_date_count'] = round(df_results['count_stop_loss_date'] / df_results['count_buy_date']*100, 2)
                                df_results['maturity_date_count_per_buy_date_count'] = round(df_results['count_maturity_date'] / df_results['count_buy_date']*100, 2)
                                df_results['search_years'] = sy
                                df_results['last_price_date'] = last_price_date
                                df_results[symbol_var] = symbol
                                df_results[name_var] = stock_name
                                df_results[type_var] = stock_type

                                best_win_condition = df_results.loc[df_results['win_rate'].idxmax()].copy()
                                best_revenue_condition = df_results.loc[df_results['revenue_rate'].idxmax()].copy()
                                best_revenue_days_held_condition = df_results.loc[df_results['avg_revenue_per_days_held'].idxmax()].copy()

                                # 최적 조건 데이터 추가
                                final_best_win_df = pd.concat(
                                    [final_best_win_df, pd.DataFrame([best_win_condition])], 
                                    ignore_index=True
                                    )
                                
                                final_best_return_df = pd.concat(
                                    [final_best_return_df, pd.DataFrame([best_revenue_condition])],
                                    ignore_index=True
                                    )
                                final_best_return_per_days_held_df = pd.concat(
                                    [final_best_return_per_days_held_df, pd.DataFrame([best_revenue_days_held_condition])],
                                    ignore_index=True
                                    )
                                    
                                # 최적 조건 결과 저장
                                final_best_win_df.to_csv(win_save_path, index=False, encoding='utf-8-sig')
                                final_best_return_df.to_csv(return_save_path, index=False, encoding='utf-8-sig')
                                final_best_return_per_days_held_df.to_csv(return_per_days_held_save_path, index=False, encoding='utf-8-sig')
                        
                        except Exception as e:
                            print(f"Error processing symbol {symbol}: {e}")
                            traceback.print_exc()
                            continue

                        # 진행 상태 업데이트
                        processed_symbols.append(symbol)
                        pd.DataFrame({symbol_var: processed_symbols}, dtype=str).to_csv(progress_file_path, index=False)

                        done_count += 1
                        print(f"Processed and passed symbol: {symbol}, {done_count}/{len(all_symbols)}")
                    else:
                        print(f"No trades found for symbol {symbol}. Skipping.")
                except Exception as e:
                    traceback.print_exc()
                    print(f"Error processing future for symbol {futures[future]}: {e}")

    finally:
        # Stop resource monitoring
        try:
            stop_monitoring(stop_event)
        except Exception as e:
            print(f"Error stopping monitoring: {e}")
            traceback.print_exc()


    func_end_datetime = datetime.now()
    message = f'End. process_all_stocks_with_save_optimized, Time: {func_end_datetime.strftime("%Y-%m-%d %H:%M:%S")}, running minutes: {int((func_end_datetime - func_start_datetime).total_seconds() / 60)}'
    send_simple_message(message)
    print(message)

# def create_trade_signal_by_investment_target():
