import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from config import DefaultValueConfig as dvc, LocalFilePathConfig as lfpc, HankookConfig as hc

user_info1 = hc.kr_account['account1']
user_info2 = hc.kr_account['account2']

from hankook_api import KISAPIClient

from slack_message import send_simple_message

import FinanceDataReader as fdr

import psutil
import threading
import time
from krxholidays import is_holiday

from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

import os
import re
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

daily_reco_win_path = lfpc.daily_reco_win_path
daily_reco_revenue_path = lfpc.daily_reco_revenue_path
daily_reco_revenue_per_days_held_path = lfpc.daily_reco_revenue_per_days_held_path

daily_order_path = lfpc.daily_order_path

# Example Usage
investment_target_best_csv_paths = {
    'win_rate': daily_best_win_csvs_path,
    'revenue_rate': daily_best_return_csvs_path,
    'revenue_per_days_held': daily_best_return_per_days_held_csvs_path
}

tax_rate = 0.0018
fee_rate = 0.00007

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
    df_csv = pd.read_csv(save_path, dtype=str)
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
        
        client = KISAPIClient(hc.kr_account['account1'])
        
        kr_stock_df = fdr.StockListing('KRX')
        kr_stock_df[type_var] = 'stock'
        kr_stock_df[name_var] = kr_stock_df[is_name_uppercase]
        kr_stock_df[symbol_var] = kr_stock_df[is_code_uppercase] # fdr.StockListing('KRX')  return Code
        kr_stock_df[total_stock_var] = kr_stock_df[is_total_stock_uppercase].astype(float).astype('int64')
        kr_stock_df['marcap'] = kr_stock_df['Marcap']

        krx_administrative_df = fdr.StockListing('KRX-ADMINISTRATIVE')
        krx_administrative_df[symbol_var] = krx_administrative_df[is_symbol_uppercase]
        krx_administrative_symbol_list = krx_administrative_df[symbol_var].unique().tolist()
        kr_stock_df = kr_stock_df[~kr_stock_df[symbol_var].isin(krx_administrative_symbol_list)].reset_index(drop=True)

        kr_etf_df = fdr.StockListing('ETF/KR')
        kr_etf_df[type_var] = 'etf'
        kr_etf_df[name_var] = kr_etf_df[is_name_uppercase]
        kr_etf_df[symbol_var] = kr_etf_df[is_symbol_uppercase]
        kr_etf_df['Price'] = kr_etf_df['Price'].astype('int64')
        kr_etf_df[total_stock_var] = None
        kr_etf_df['marcap'] = None
        
        kr_etf_df[total_stock_var] = 0

        for etf_symbol in kr_etf_df[symbol_var].unique().tolist():

            try:
                t_df = client.search_stock_info(PDNO=str(etf_symbol))
                t_total_stock = int(float(t_df[lstg_stqt_var].iloc[0]))
                t_market_cap = t_total_stock * kr_etf_df['Price']
                kr_etf_df[total_stock_var] = np.where(kr_etf_df[symbol_var] == etf_symbol, t_total_stock, kr_etf_df[total_stock_var])
                kr_etf_df['marcap'] = np.where(kr_etf_df[symbol_var] == etf_symbol, t_market_cap, kr_etf_df['marcap'])
            except:
                pass

        krx_symbol_list_df = pd.concat([kr_stock_df, kr_etf_df])
        krx_symbol_list_df[date_var] = end_date
        krx_symbol_list_df[total_stock_var] = krx_symbol_list_df[total_stock_var].astype(float).astype('int64')
        krx_symbol_list_df = krx_symbol_list_df[krx_symbol_list_df[total_stock_var]>0].reset_index(drop=True)

        new_df = krx_symbol_list_df[[type_var, symbol_var, name_var, total_stock_var, 'marcap']].reset_index(drop=True)

        if save_dummy == '1':
            os.makedirs(file_path, exist_ok=True)
            if not new_df.empty:
                new_df.to_csv(save_path, index=False, encoding='utf-8-sig', quoting=1)
            else:
                print(f"Warning: Attempted to save an empty DataFrame to {save_path}.")

        return new_df


    if read_dummy == '1':
        if os.path.exists(save_path):
            return_df = pd.read_csv(save_path, dtype=str)
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
            df = pd.read_csv(min_date_file, index_col=symbol_var, dtype=str)
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

    df_min_dates.to_csv(min_date_file, encoding='utf-8-sig', quoting=1)

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
            df_csv = pd.read_csv(save_path, dtype=str)
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
                            df_new_price.to_csv(save_path, index=False, encoding='utf-8-sig', quoting=1)
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
                    df_updated.to_csv(save_path, index=False, encoding='utf-8-sig', quoting=1)
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
                    df_new_price.to_csv(save_path, index=False, encoding='utf-8-sig', quoting=1)
                else:
                    print(f"Warning: Attempted to save an empty DataFrame to {save_path}.")
            func_end_datetime = datetime.now()
            func_end_date_str = func_end_datetime.strftime('%Y-%m-%d %H:%M:%S')
            func_run_diff_seconds = int(round((func_end_datetime - func_start_datetime).total_seconds(),0))
            # print(f'End. update_daily_stock_price, Time: {func_end_date_str}, running seconds:{func_run_diff_seconds}')
            return df_new_price
    

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


def update_cci_data(symbol, read_dummy, save_dummy, end_date_str):
    file_path = lfpc.daily_cci_index_csvs_path
    file_name = f'kr_cci_symbol_{symbol}.csv'
    save_path = os.path.join(file_path, file_name)

    # Ensure the directory exists
    os.makedirs(file_path, exist_ok=True)

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
                        df_cci = pd.read_csv(save_path, dtype=str)
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
                    df_combined.to_csv(save_path, index=False, encoding='utf-8-sig', quoting=1)
                return df_combined
            else:
                # print(f"No new CCI data to update for symbol {symbol}.")
                return df_cci
        else:
            df_new_cci = create_new_cci_data(df_price)
            if save_dummy == '1':
                df_new_cci.to_csv(save_path, index=False, encoding='utf-8-sig', quoting=1)
            return df_new_cci


# 데이터에서 날짜 컬럼을 확인하고 날짜 형식으로 변환
def ensure_datetime_format(df, date_column=date_var):
    if date_column in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df[date_column]):
            df[date_column] = pd.to_datetime(df[date_column], errors='coerce', format='ISO8601')
    return df

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
    df_all_trades.to_csv(trades_save_path, index=False, encoding='utf-8-sig', quoting=1)

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

        stock_symbols = df_kr[df_kr[type_var]=='stock'].loc[:299][symbol_var].tolist() ## Note : 전체 주식 중 300개에 대해서만 거래 시뮬레이션 구동
        etf_symbols = df_kr[df_kr[type_var]=='etf'].reset_index(drop=True).loc[:299][symbol_var].tolist() ## Note : 전체 ETF 중 300개에 대해서만 거래 시뮬레이션 구동

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
            final_best_win_df = pd.read_csv(win_save_path, dtype=str)
        else:
            final_best_win_df = pd.DataFrame()

        if os.path.exists(return_save_path):
            final_best_return_df = pd.read_csv(return_save_path, dtype=str)
        else:
            final_best_return_df = pd.DataFrame()

        if os.path.exists(return_per_days_held_save_path):
            final_best_return_per_days_held_df = pd.read_csv(return_per_days_held_save_path, dtype=str)
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
                            pd.DataFrame({symbol_var: processed_symbols}, dtype=str).to_csv(progress_file_path, index=False, encoding='utf-8-sig', quoting=1)
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
                                    total_buy_price = ('buy_price', 'sum'),
                                    total_reach_target_price = ('reach_target_price', 'sum'),
                                    total_stop_loss_price = ('stop_loss_price', 'sum'),
                                    total_maturity_price = ('maturity_price', 'sum'),
                                    total_sell_price = ('sell_price', 'sum')
                                    ).reset_index()

                                df_results['win_rate'] = round(df_results['count_win'] / df_results['count_buy_date'] * 100, 2)
                                df_results['lose_rate'] = round(df_results['count_lose'] / df_results['count_buy_date'] * 100, 2)


                                df_results['total_revenue'] = round(df_results['total_sell_price'] - df_results['total_buy_price'], 0)
                                df_results['revenue_rate'] = round((df_results['total_revenue'] / df_results['total_buy_price'])*100, 2)
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
                                final_best_win_df.to_csv(win_save_path, index=False, encoding='utf-8-sig', quoting=1)
                                final_best_return_df.to_csv(return_save_path, index=False, encoding='utf-8-sig', quoting=1)
                                final_best_return_per_days_held_df.to_csv(return_per_days_held_save_path, index=False, encoding='utf-8-sig', quoting=1)
                        
                        except Exception as e:
                            print(f"Error processing symbol {symbol}: {e}")
                            traceback.print_exc()
                            continue

                        # 진행 상태 업데이트
                        processed_symbols.append(symbol)
                        pd.DataFrame({symbol_var: processed_symbols}, dtype=str).to_csv(progress_file_path, index=False, encoding='utf-8-sig', quoting=1)

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

def get_latest_best_file(directory, date_format="%Y%m%d"):
    """
    특정 디렉토리 내 파일 목록에서 파일명에 포함된 날짜를 기준으로 가장 최신 파일을 반환합니다.
    
    :param directory: 파일이 저장된 디렉토리 경로
    :param date_format: 파일명에 포함된 날짜의 형식 (기본값: YYYYMMDD)
    :return: 가장 최신 파일의 경로 또는 None
    """
    latest_file = None
    latest_date = None

    # 디렉토리 내 파일 목록 가져오기
    try:
        files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    except FileNotFoundError:
        print(f"디렉토리 {directory}를 찾을 수 없습니다.")
        return None

    # 파일명에서 날짜 추출 및 최신 파일 찾기
    for file in files:
        # 정규표현식으로 날짜 추출
        match = re.search(r'\d+', file)
        if match:
            try:
                file_date = datetime.strptime(match.group(), date_format)
                if latest_date is None or file_date > latest_date:
                    latest_date = file_date
                    latest_file = file
            except ValueError:
                # 날짜 형식에 맞지 않는 경우 무시
                continue

    if latest_file:
        return os.path.join(directory, latest_file)
    else:
        traceback.print_exc()
        print("파일명에서 날짜를 찾을 수 없거나 유효한 파일이 없습니다.")
        return None


def get_daily_signal_recommendations_sub(best_file_path):
    recent_path = get_latest_best_file(best_file_path)

    df_recent = pd.read_csv(recent_path, dtype=str)
    candidate_list = df_recent[symbol_var].unique().tolist()

    today_reco_data =[]

    for symbol in candidate_list:
        df_symbol = df_recent[df_recent[symbol_var]==symbol].iloc[0]
        name = df_symbol[name_var]
        condition_target_return = df_symbol['condition_target_return']
        condition_holding_days = df_symbol['condition_holding_days']
        condition_buy_cci_threshold = df_symbol['condition_buy_cci_threshold']
        condition_stop_loss_cci_threshold = df_symbol['condition_stop_loss_cci_threshold']
        win_rate = df_symbol['win_rate']
        count_win = df_symbol['count_win']
        revenue_rate = df_symbol['revenue_rate']
        avg_revenue_per_days_held = df_symbol['avg_revenue_per_days_held']
        avg_days_held = df_symbol['avg_days_held']

        df_price = fdr.DataReader(symbol)
        buy_price = df_price.iloc[-1][is_open_uppercase]

        df = update_cci_data(symbol, '0', '0', '')
        yesterday_open_cci = df.iloc[-2][open_cci_index_var]
        current_open_cci = df.iloc[-1][open_cci_index_var]
        if yesterday_open_cci < condition_buy_cci_threshold and current_open_cci >= condition_buy_cci_threshold:
            reco_dict = {
                symbol_var : symbol,
                name_var : name,
                'buy_price' : buy_price,
                'current_open_cci' : current_open_cci,
                'condition_target_return' : condition_target_return,
                'condition_holding_days' : condition_holding_days,
                'condition_buy_cci_threshold' : condition_buy_cci_threshold,
                'condition_stop_loss_cci_threshold' : condition_stop_loss_cci_threshold,
                'win_rate' : win_rate,
                'count_win' : count_win,
                'revenue_rate' : revenue_rate,
                'avg_revenue_per_days_held' : avg_revenue_per_days_held,
                'avg_days_held' : avg_days_held,

            }
            today_reco_data.append(reco_dict)

    if len(today_reco_data) == 0:
        df_reco = pd.DataFrame()
    else:
        df_reco = pd.DataFrame(today_reco_data)
        if symbol_var in df_reco.columns:
            df_reco[symbol_var] = df_reco[symbol_var].astype('string')

    return df_reco

def get_daily_signal_recommendations():
    try:
        save_date_str = datetime.now().strftime('%Y%m%d')

        # Ensure directories exist
        os.makedirs(daily_reco_win_path, exist_ok=True)
        os.makedirs(daily_reco_revenue_path, exist_ok=True)
        os.makedirs(daily_reco_revenue_per_days_held_path, exist_ok=True)

        df_win_reco = get_daily_signal_recommendations_sub(best_file_path=daily_best_win_csvs_path)
        df_win_reco.to_csv(f"{daily_reco_win_path}/reco_win_{save_date_str}.csv", index=False, encoding='utf-8-sig', quoting=1)

        df_revenue_reco = get_daily_signal_recommendations_sub(best_file_path=daily_best_return_csvs_path)
        df_revenue_reco.to_csv(f"{daily_reco_revenue_path}/reco_revenue_{save_date_str}.csv", index=False, encoding='utf-8-sig', quoting=1)

        df_revenue_per_days_held_reco = get_daily_signal_recommendations_sub(best_file_path=daily_best_return_per_days_held_csvs_path)
        df_revenue_per_days_held_reco.to_csv(f"{daily_reco_revenue_per_days_held_path}/reco_revenue_per_days_held_{save_date_str}.csv", index=False, encoding='utf-8-sig', quoting=1)

    except FileNotFoundError as fnfe:
        print(f"FileNotFoundError: {fnfe}")
        traceback.print_exc()
    except pd.errors.EmptyDataError as ede:
        print(f"EmptyDataError: {ede}")
        traceback.print_exc()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        traceback.print_exc()


def get_candidate_list(investment_target, csv_paths):
    """
    투자 대상에 따라 적합한 후보 리스트를 반환합니다.
    
    Parameters:
        investment_target (str): 'win_rate', 'revenue_rate', or 'revenue_per_days_held'.
        csv_paths (dict): 각 투자 대상별 CSV 경로 딕셔너리.
        
    Returns:
        list: 후보 심볼 리스트.
    """
    # CSV 파일 경로 설정
    target_to_path = {
        'win_rate': csv_paths.get('win_rate'),
        'revenue_rate': csv_paths.get('revenue_rate'),
        'revenue_per_days_held': csv_paths.get('revenue_per_days_held')
    }
    
    if investment_target not in target_to_path:
        raise ValueError(f"Invalid investment target: {investment_target}")
    
    best_file_path = target_to_path[investment_target]
    recent_file_path = get_latest_best_file(best_file_path)

    # CSV 파일 읽기
    df_recent = pd.read_csv(recent_file_path, dtype=str)

    float_columns = [criteria_var1, criteria_var2, criteria_var3]

    for col in float_columns:
        df_recent[col] = df_recent[col].astype(float)

    # Threshold 설정
    target_to_threshold = {
        'win_rate': ('win_rate', 'count_win', 'avg_days_held'),
        'revenue_rate': ('revenue_rate', 'count_win', 'avg_days_held'),
        'revenue_per_days_held': ('avg_revenue_per_days_held', 'win_rate', 'avg_days_held')
    }
    
    criteria_var1, criteria_var2, criteria_var3 = target_to_threshold[investment_target]
    
    candidate_threshold1 = df_recent[criteria_var1].quantile(0.7)  
    candidate_threshold2 = df_recent[criteria_var2].quantile(0.3)
    candidate_threshold3 = df_recent[criteria_var3].quantile(0.3)

    # 후보 리스트 생성
    candidate_list = df_recent[
        (df_recent[criteria_var1] >= candidate_threshold1) & 
        (df_recent[criteria_var2] >= candidate_threshold2) &
        (df_recent[criteria_var3] <= candidate_threshold3)

    ][symbol_var].unique().tolist()

    return df_recent, candidate_list


@log_function_call
def create_buy_order_data(investment_target, user_info):

    client = KISAPIClient(user_info)

    cano = user_info['CANO']
    df_recent, candidate_list = get_candidate_list(
        investment_target=investment_target,
        csv_paths=investment_target_best_csv_paths
    )

    order_index = 1
    df_real_history = pd.DataFrame()

    os.makedirs(f'{daily_order_path}', exist_ok=True)
    order_history_csv_path = f'{daily_order_path}/real_order_history_account_{cano}.csv'
    try:
        if os.path.exists(order_history_csv_path):
            df_real_history = pd.read_csv(order_history_csv_path, dtype=str)
            if not df_real_history.empty:
                df_real_history = pd.read_csv(order_history_csv_path, dtype=str)
                existed_last_order_index = len(df_real_history)
                order_index = existed_last_order_index+1
        else:
            pass

        order_data = []

        order_date = datetime.now().date()
        
        for symbol in candidate_list:
            add_index = int(candidate_list.index(symbol)) + 1
            try:
                # 최근 데이터 가져오기
                df_symbol = df_recent[df_recent[symbol_var] == symbol].iloc[0]
                symbol_name = df_symbol[name_var]
                symbol_type = df_symbol[type_var]
                
                # 조건 및 데이터 초기화
                condition_params = {
                    "target_return": float(df_symbol['condition_target_return']),
                    "holding_days": int(df_symbol['condition_holding_days']),
                    "buy_cci_threshold": int(df_symbol['condition_buy_cci_threshold']),
                    "stop_loss_cci_threshold": int(df_symbol['condition_stop_loss_cci_threshold'])
                }
                
                performance_metrics = {
                    "win_rate": float(df_symbol['win_rate']),
                    "count_win": int(df_symbol['count_win']),
                    "revenue_rate": float(df_symbol['revenue_rate']),
                    "avg_revenue_per_days_held": int(df_symbol['avg_revenue_per_days_held']),
                    "avg_days_held": int(df_symbol['avg_days_held']),
                    "total_revenue": int(df_symbol['total_revenue'])
                }

                # 가격 데이터 불러오기
                df_price = fdr.DataReader(symbol=symbol).reset_index()
                df_price = df_price.rename(columns={
                    is_date_uppercase: date_var,
                    is_open_uppercase: open_pr_var,
                    is_close_uppercase: close_pr_var,
                    is_high_uppercase: high_pr_var,
                    is_low_uppercase: low_pr_var,
                    is_volume_uppercase: daily_trade_stock_var
                })

                # CCI 데이터 생성
                df_cci = create_new_cci_data(df_price=df_price)
                df_current = df_cci.iloc[-1]

                # 매수 조건 확인
                if (
                    df_cci.iloc[-2][open_cci_index_var] < condition_params["buy_cci_threshold"]
                    and df_cci.iloc[-1][open_cci_index_var] >= condition_params["buy_cci_threshold"]
                ):

                    buy_order_price = df_current[close_pr_var]
                    buy_order_qty = None  
                    buy_order_number = None

                    # 잔고 확인 및 주문 실행
                    try:
                        df1, df2, balance_summary = client.get_stock_balance(CANO=cano, ACNT_PRDT_CD='01')
                        budget = float(balance_summary['prvs_rcdl_excc_amt'])
                        target_budget = 0

                        if budget is not None:
                            target_budget = int(float(budget * 0.1))
                        buy_order_qty = int(round(float(target_budget) / float(buy_order_price), 0))

                        if buy_order_qty >= 1:
                            order_result = client.place_order(
                            PDNO=symbol,
                            CANO=cano,
                            ORD_DVSN='00',
                            ORD_QTY=f'{buy_order_qty}',
                            ORD_UNPR=f'{buy_order_price}',
                            order_type='buy'
                        )
                            buy_order_number = str(order_result['ODNO'])
                            # 실제 예산 기반 주문 수량 계산 로직 추가 가능
                            time.sleep(0.05)  # 요청 제한 조절
                    except Exception as e:
                        traceback.print_exc()
                        pass

                    order_data.append({
                        'order_index': order_index+add_index,
                        'buy_order_date': order_date.strftime('%Y-%m-%d'),
                        symbol_var: symbol,
                        name_var: symbol_name,
                        **condition_params,
                        **performance_metrics,
                        'buy_order_number': buy_order_number,
                        'buy_order_price': buy_order_price,
                        'buy_order_qty': buy_order_qty,
                        'real_buy_date': None,
                        'real_buy_price': None,
                        'real_buy_qty': None,
                        'sell_order_date': None,
                        'sell_order_number': None,
                        'sell_order_price': None,
                        'sell_order_qty' : None,
                        'maturity_date': None,
                        'real_sell_signal':None,
                        'real_sell_date': None,
                        'real_sell_price': None,
                        'real_sell_qty': None,
                        'real_revenue': None,
                        'real_revenue_rate': None,
                        'real_revenue_per_days_held': None,
                        'real_days_held' : None,
                        'trade_result': None,
                        'investment_target': investment_target,
                        type_var : symbol_type,
                    })

            except Exception as e:
                traceback.print_exc()
                continue

        df_order = pd.DataFrame(order_data)

        date_colmuns = ['buy_order_date', 'real_buy_date', 'sell_order_date', 'real_sell_date', 'maturity_date']

        if not df_order.empty:
            try:
                for col in date_colmuns:
                    df_order[col] = pd.to_datetime(df_order[col], errors='coerce', format='ISO8601')
            except Exception as e:
                traceback.print_exc()
                pass
            if len(df_real_history)>0:
                for col in date_colmuns:
                    df_real_history[col] = pd.to_datetime(df_real_history[col], errors='coerce', format='ISO8601')

                # `df_order`와 `df_real_history` 모두 유효한지 확인
                if not df_order.empty and not df_order.isna().all(axis=None):
                    if not df_real_history.empty and not df_real_history.isna().all(axis=None):
                        # 둘 다 유효한 경우 병합
                        df_real_history = pd.concat([df_real_history, df_order], axis=0, ignore_index=True)
                    else:
                        # 기존 데이터가 없을 경우 새 데이터만 사용
                        df_real_history = df_order
                else:
                    if not df_real_history.empty:
                        # 새 데이터가 없을 경우 기존 데이터만 사용
                        df_real_history = df_real_history
                    else:
                        print("Warning: No valid data to process.")
                        return pd.DataFrame()  # 빈 DataFrame 반환

                # 중복 제거 및 저장
                df_real_history = df_real_history.dropna(subset=['buy_order_number'], axis=0, how='all')
                df_real_history = df_real_history.drop_duplicates(subset=['buy_order_number'], keep='last')
                df_real_history.to_csv(order_history_csv_path, index=False, encoding='utf-8-sig', quoting=1)

            else:
                # 기존 파일이 없으면 새로운 데이터 저장
                if not df_order.empty:
                    df_order.to_csv(order_history_csv_path, index=False, encoding='utf-8-sig', quoting=1)
                    df_real_history = df_order
                else:
                    print("Warning: No data to save.")
                    return pd.DataFrame()  # 빈 DataFrame 반환
    except:
        traceback.print_exc()
        pass

    return df_real_history

@log_function_call
def run_buy_order():
    now = datetime.now()
    if not is_holiday(now):
        try:
            investment_targets = ['win_rate', 'revenue_rate', 'revenue_per_days_held']
            # investment_targets = ['win_rate']
            for investment_target in investment_targets:
                df_updated = create_buy_order_data(investment_target=investment_target, user_info=user_info1)
                df_updated = create_buy_order_data(investment_target=investment_target, user_info=user_info2)
        except Exception as e:
            traceback.print_exc()
            pass

@log_function_call
def check_buy_order_execution(user_info):
    
    client = KISAPIClient(user_info)

    check_date_str = datetime.now().strftime('%Y%m%d')
    cano = user_info['CANO']
    df_execution, summary_dict = client.get_daily_order_execution(cano, check_date_str, check_date_str)
    
    if len(df_execution) > 0:
        df_execution['odno'] = df_execution['odno'].astype(int).astype(str)  # 00001111 형태를 1111형태로 바꿔서 맞춤

    try:
        save_path = f'{daily_order_path}/real_order_history_account_{cano}.csv'

        date_colmuns = ['buy_order_date', 'real_buy_date', 'sell_order_date', 'real_sell_date', 'maturity_date']

        df_real_history = pd.read_csv(save_path, dtype=str)

        if not df_real_history.empty:
            try:
                for col in date_colmuns:
                    df_real_history[col] = pd.to_datetime(df_real_history[col], errors='coerce', format='ISO8601')
            except Exception as e:
                traceback.print_exc()
                pass
        
        if not df_execution.empty:
            for i, x in df_execution.iterrows():
                order_date = pd.to_datetime(x['ord_dt'], errors='coerce', format='ISO8601')
                buy_order_number = str(int(float(x['odno'])))
                print(buy_order_number)
                sell_buy_dvsn_cd = x['sll_buy_dvsn_cd'] # 01 매수 02 매도
                tot_ccld_qty = int(round(float(x['tot_ccld_qty']), 0)) # 총체결수량
                avg_prvs = int(round(float(x['avg_prvs']), 0)) # 체결평균가 ( 총체결금액 / 총체결수량)
                
                if sell_buy_dvsn_cd == '02': ## 매도 01 매수 02
                    print(df_real_history.dtypes)
                    condition = df_real_history['buy_order_number']==buy_order_number
                    print(df_real_history[condition])
                    df_real_history.loc[condition, 'real_buy_date'] = order_date.strftime('%Y-%m-%d')
                    print(buy_order_number)
                    print(avg_prvs)
                    print(tot_ccld_qty)
                    df_real_history.loc[condition, 'real_buy_price'] = avg_prvs
                    df_real_history.loc[condition, 'real_buy_qty'] = tot_ccld_qty
                    
                    if len(df_real_history[condition]['holding_days']) == 0:
                        pass
                    else:
                        maturity_date = order_date + timedelta(days=int(df_real_history[condition]['holding_days'].iloc[0].astype('int64')))
                        df_real_history.loc[condition, 'maturity_date'] = maturity_date.strftime('%Y-%m-%d')
                    

        df_real_history.to_csv(save_path, index=False, encoding='utf-8-sig', quoting=1)

        return df_real_history
    except:
        traceback.print_exc()
        pass

@log_function_call
def create_sell_order_data(user_info):

    client = KISAPIClient(user_info)

    cano = user_info['CANO']
    save_path = f'{daily_order_path}/real_order_history_account_{cano}.csv'

    date_colmuns = ['buy_order_date', 'real_buy_date', 'sell_order_date', 'real_sell_date', 'maturity_date']
    if os.path.exists(save_path):
        try:
            df_real_history = pd.read_csv(save_path, dtype=str)
            
            if not df_real_history.empty:
                try:
                    for col in date_colmuns:
                        df_real_history[col] = pd.to_datetime(df_real_history[col], errors='coerce', format='ISO8601')

                    today_date = pd.Timestamp(datetime.now().date())
                    df_possible_sell_order = df_real_history[df_real_history['sell_order_date'].isna()]

                    for i, x in df_possible_sell_order.iterrows():
                        symbol = x[symbol_var]
                        
                        real_buy_price = x['real_buy_price']
                        real_buy_qty = str(int(float(x['real_buy_qty'])))
                        maturity_date = pd.Timestamp(x['maturity_date'])
                        target_return = x['target_return']
                        stop_loss_cci_threshold = x['stop_loss_cci_threshold']

                        target_return_rate = (float(target_return) + tax_rate + fee_rate) / 100
                        target_price = int(round(float(real_buy_price) * (1 + target_return_rate), 0))

                        df_price = fdr.DataReader(symbol).reset_index()

                        df_price[date_var] = df_price[is_date_uppercase]
                        df_price[open_pr_var] = df_price[is_open_uppercase]
                        df_price[close_pr_var] = df_price[is_close_uppercase]
                        df_price[high_pr_var] = df_price[is_high_uppercase]
                        df_price[low_pr_var] = df_price[is_low_uppercase]
                        df_price[daily_trade_stock_var] = df_price[is_volume_uppercase]
                        df_price[date_var] = pd.to_datetime(df_price[date_var], errors='coerce', format='ISO8601')

                        df_cci = create_new_cci_data(df_price)

                        sell_order_date = None
                        sell_order_number = ''
                        real_sell_signal = None
                        sell_order_price = '0'
                        sell_order_qty = str(int(float(real_buy_qty)))

                        ORD_DVSN = '01'
                        if ORD_DVSN == '01' or ORD_DVSN == '06': # 시장가, 장후 시간외 종가
                            sell_order_price = '0'
                        elif ORD_DVSN == '07': # 시간외 단일가
                            sell_order_price = str(int(round(float(df_price.iloc[-1][close_pr_var]), 0)))
                        # elif ORD_DVSN == '02' : # 조건부 지정가 

                        try:
                            if today_date >= maturity_date:
                                result = client.place_order(PDNO=symbol, CANO=cano, ORD_DVSN=ORD_DVSN, ORD_QTY=real_buy_qty, ORD_UNPR=sell_order_price, order_type='sell')
                                sell_order_number = str(int(result['odno']))
                                real_sell_signal = 'maturity'
                                sell_order_date = datetime.now().strftime('%Y-%m-%d')
                            else:
                                if df_price[close_pr_var].iloc[-1] > target_price:
                                    result = client.place_order(PDNO=symbol, CANO=cano, ORD_DVSN=ORD_DVSN, ORD_QTY=real_buy_qty, ORD_UNPR=sell_order_price, order_type='sell')
                                    sell_order_number = str(int(result['odno']))
                                    real_sell_signal = 'reach_target'
                                    sell_order_date = datetime.now().strftime('%Y-%m-%d')
                                elif df_cci[close_cci_index_var].iloc[-1] <= stop_loss_cci_threshold:
                                    result = client.place_order(PDNO=symbol, CANO=cano, ORD_DVSN=ORD_DVSN, ORD_QTY=real_buy_qty, ORD_UNPR=sell_order_price, order_type='sell')
                                    sell_order_number = str(int(result['odno']))
                                    real_sell_signal = 'stop_loss'
                                    sell_order_date = datetime.now().strftime('%Y-%m-%d')

                                df_real_history.loc[i, 'sell_order_date'] = sell_order_date
                                df_real_history.loc[i, 'sell_order_number'] = sell_order_number
                                df_real_history.loc[i, 'sell_order_price'] = float(sell_order_price)
                                df_real_history.loc[i, 'sell_order_qty'] = float(sell_order_qty)
                                df_real_history.loc[i, 'real_sell_signal'] = real_sell_signal
                            
                        except Exception as e:
                            traceback.print_exc()
                            pass

                    df_real_history.to_csv(save_path, index=False, encoding='utf-8-sig', quoting=1)

                except Exception as e:
                    traceback.print_exc()
                    pass

        except pd.errors.EmptyDataError:
            traceback.print_exc()

    return df_real_history

@log_function_call
def check_sell_order_execution(user_info):

    client = KISAPIClient(user_info)

    cano = user_info['CANO']
    execute_date_str = datetime.now().strftime('%Y%m%d')
    
    df_execution, summary_dict = client.get_daily_order_execution(cano, execute_date_str, execute_date_str)
    
    if len(df_execution) > 0:
        df_execution['odno'] = df_execution['odno'].astype('int64').astype(str)

    save_path = f'{daily_order_path}/real_order_history_account_{cano}.csv'
    date_colmuns = ['buy_order_date', 'real_buy_date', 'sell_order_date', 'real_sell_date', 'maturity_date']
    if os.path.exists(save_path):
        df_real_history = pd.read_csv(save_path, dtype=str)

        if not df_real_history.empty:
            try:
                for col in date_colmuns:
                    df_real_history[col] = pd.to_datetime(df_real_history[col], errors='coerce', format='ISO8601')
            except Exception as e:
                traceback.print_exc()
                pass

    for i, x in df_real_history.iterrows():
        real_buy_date = x['real_buy_date']
        real_buy_price = x['real_buy_price']
        if pd.isna(real_buy_price):
            pass
        else:
            real_buy_price = int(float(real_buy_price))
        
        sell_order_number = x['sell_order_number']
        
        if pd.isna(sell_order_number):
            pass
        else:
            try:
                sell_order_number = int(float(sell_order_number))
                if not df_execution.empty:
                    # 필터링된 결과가 비어 있지 않은지 확인
                    df_filtered = df_execution[df_execution['odno'] == sell_order_number]
                    if not df_filtered.empty:
                        df_real_sell = df_filtered.iloc[0].copy()
                        df_real_sell['ord_dt'] = pd.to_datetime(df_real_sell['ord_dt'], errors='coerce', format='ISO8601')
                        
                        real_sell_date = df_real_sell['ord_dt']
                        sell_order_number = int(df_real_sell['odno'])
                        sell_buy_dvsn_cd = df_real_sell['sll_buy_dvsn_cd']  # 01 매수, 02 매도
                        real_sell_qty = int(round(float(df_real_sell['tot_ccld_qty']), 0))  # 총체결수량
                        real_sell_price = int(round(float(df_real_sell['avg_prvs']), 0))  # 체결평균가
                        
                        fee_amount = round(((real_sell_price * real_sell_qty) * (fee_rate)), 0)
                        tax_amount = round(((real_sell_price * real_sell_qty) * (tax_rate)), 0)
                        real_revenue = int(((real_sell_price - real_buy_price) - (fee_amount + tax_amount)) / real_sell_qty)
                        
                        real_revenue_rate = ((real_revenue) / real_buy_price) * 100
                        
                        real_days_held = (real_sell_date - real_buy_date).days + 1
                        real_revenue_per_days_held = round(float(real_revenue / real_days_held), 2)

                        if sell_buy_dvsn_cd == '01':  # 01 매도
                            df_real_history.loc[i, 'real_sell_date'] = real_sell_date.strftime('%Y-%m-%d')
                            df_real_history.loc[i, 'real_sell_price'] = real_sell_price
                            df_real_history.loc[i, 'real_sell_qty'] = real_sell_qty
                            df_real_history.loc[i, 'real_revenue'] = real_revenue
                            df_real_history.loc[i, 'real_revenue_rate'] = real_revenue_rate
                            df_real_history.loc[i, 'real_revenue_per_days_held'] = real_revenue_per_days_held
                            df_real_history.loc[i, 'real_days_held'] = real_days_held
                    else:
                        print(f"No matching execution found for sell_order_number: {sell_order_number}")
            except Exception as e:
                print(f"Error processing sell order number: {sell_order_number}")
                traceback.print_exc()

    df_real_history.to_csv(save_path, index=False, encoding='utf-8-sig', quoting=1)

    return df_real_history

@log_function_call
def run_sell_order():
    now = datetime.now()
    if not is_holiday(now):
        try:
            df_real_history = check_buy_order_execution(user_info1)
            df_real_history = create_sell_order_data(user_info1)

            df_real_history = check_buy_order_execution(user_info2)
            df_real_history = create_sell_order_data(user_info2)
        except Exception as e:
            traceback.print_exc()
            pass

@log_function_call
def update_order_execution():
    now = datetime.now()
    if not is_holiday(now):
        try:
            df = check_buy_order_execution(user_info1)
            df = check_sell_order_execution(user_info1)

            df = check_buy_order_execution(user_info2)
            df = check_sell_order_execution(user_info2)
        except Exception as e:
            traceback.print_exc()
            pass