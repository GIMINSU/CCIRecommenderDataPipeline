import os
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from daily_function import create_kr_symbol_list, process_all_stocks_with_save_optimized, get_daily_signal_recommendations, run_buy_order, run_sell_order, update_order_execution
from slack_message import send_simple_message
from krxholidays import is_holiday
import logging
import traceback


# 로그 파일 경로 설정
log_file_path = os.path.join(os.path.dirname(__file__), "app.log")

# 기존 핸들러 제거 및 로그 설정
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path),  # 파일에 로그 저장
        logging.StreamHandler()  # 콘솔에 로그 출력
    ]
)

# Flask 앱 생성
app = Flask(__name__)

# Flask 로거 설정
app.logger.handlers = logging.getLogger().handlers
app.logger.setLevel(logging.INFO)

# Configurations
CONFIG = {
    "read_dummy": "1",
    "save_dummy": "1",
    "end_date_str": datetime.now().strftime('%Y%m%d'),
    "holding_days": [10, 20, 30, 40, 50, 60],
    "target_return_values": range(1, 11),
    "buy_cci_thresholds": [300, 250, 200, 150, 100, 50, 0, -50, -100, -150, -200, -250, -300],
    "stop_loss_cci_thresholds": [300, 250, 200, 150, 100, 50, 0, -50, -100, -150, -200, -250, -300],
    "search_history_years": ["all"],
}

# 주요 작업 실행
def execute_pipeline():
    now = datetime.now()
    logging.info("Pipeline execution started.")
    try:
        if not is_holiday(now):
            logging.info("Today is not a holiday. Proceeding with pipeline execution.")
            
            # Step 1: 종목 리스트 생성
            logging.info("Step 1: Running create_kr_symbol_list...")
            df_kr = create_kr_symbol_list(
                read_dummy=CONFIG["read_dummy"],
                save_dummy=CONFIG["save_dummy"],
                end_date_str=CONFIG["end_date_str"]
            )
            if df_kr is None or df_kr.empty:
                raise ValueError("create_kr_symbol_list returned an empty DataFrame.")
            logging.info("Step 1 Complete: Symbol list created!")

            # Step 2: 주식 데이터 처리
            logging.info("Step 2: Running process_all_stocks_with_save_optimized...")
            process_all_stocks_with_save_optimized(
                df_kr=df_kr,
                holding_days=CONFIG["holding_days"],
                target_return_values=CONFIG["target_return_values"],
                search_history_years=CONFIG["search_history_years"],
                buy_cci_thresholds=CONFIG["buy_cci_thresholds"],
                stop_loss_cci_thresholds=CONFIG["stop_loss_cci_thresholds"],
                read_dummy=CONFIG["read_dummy"],
                save_dummy=CONFIG["save_dummy"],
                end_date_str=CONFIG["end_date_str"]
            )
            logging.info("Step 2 Complete: All stocks processed and results saved!")
            return "Execution pipeline completed successfully!"
        else:
            logging.info("Today is a holiday. Skipping pipeline execution.")

    except Exception as e:
        error_message = traceback.format_exc()
        logging.error(f"Error during pipeline execution: {error_message}")
        send_simple_message(error_message)  # Slack 알림
        return f"Execution pipeline failed: {e}"

# APScheduler를 사용한 작업 스케줄러 설정
def setup_scheduler():
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.start()

    # 한국 시장 스케줄
    kr_best_data_trigger = CronTrigger(hour=22, minute=0)
    scheduler.add_job(execute_pipeline, trigger=kr_best_data_trigger, id="kr_best_data")

    kr_reco_data_trigger = CronTrigger(hour=9, minute=0, second=2)
    scheduler.add_job(run_buy_order, trigger=kr_reco_data_trigger, id='kr_buy_order')
    scheduler.add_job(get_daily_signal_recommendations, trigger=kr_reco_data_trigger, id='kr_reco_data')

    kr_sell_trigger = CronTrigger(hour=15, minute=29)
    scheduler.add_job(run_sell_order, trigger=kr_sell_trigger, id='kr_sell_order')

    kr_update_trigger = CronTrigger(hour=15, minute=31)
    scheduler.add_job(update_order_execution, trigger=kr_update_trigger, id='kr_execution_update')

    
    
    logging.info("Scheduler has been set up with APScheduler.")

@app.route("/", methods=["GET", "POST"])
def index():
    logging.info("Root endpoint accessed.")
    return "Flask App Running with APScheduler!"

if __name__ == '__main__':
    logging.info("Starting Flask App and Scheduler...")
    try:
        setup_scheduler()
        app.run(host="0.0.0.0", port=5500, debug=False, use_reloader=False, threaded=True)
    except Exception as e:
        logging.critical(f"Flask app failed to start: {e}")
        raise
