import pandas as pd
import requests
import json
from datetime import datetime
import time
from config import HankookConfig, DefaultValueConfig as dvc


class KISAPIClient:
    def __init__(self, user_info):
        """
        KIS API 클라이언트 초기화
        :param user_info: 사용자 정보 (dict)
            - appkey: API AppKey
            - appsecret: API AppSecret
            - CANO: 사용자 계좌번호 (10자리)
        """
        self.real_domain = HankookConfig.real_domain
        self.user_info = user_info
        self.appkey = user_info['appkey']
        self.appsecret = user_info['appsecret']

        # 변수 매핑
        self.var_mapping = {
            "date": dvc.stck_bsop_date_var,
            "close": dvc.stck_clpr_var,
            "open": dvc.stck_oprc_var,
            "high": dvc.stck_hgpr_var,
            "low": dvc.stck_lwpr_var,
        }

    def issue_access_token(self):
        """
        액세스 토큰 발급
        1. 요청 URL: https://{도메인}/oauth2/tokenP
        2. 요청 본문:
            - grant_type: client_credentials
            - appkey: AppKey
            - appsecret: AppSecret
        3. 반환값:
            - access_token: API 인증 토큰 (Bearer)
        """
        csv_file_name = f"{self.user_info['CANO']}_token.csv"
        today_date = datetime.now().date().strftime("%Y-%m-%d")

        # 기존 발급된 토큰 확인
        try:
            df = pd.read_csv(csv_file_name)
            if not df.empty and df["token_issue_date"].iloc[0] == today_date:
                return df[f"{self.user_info['CANO']}_token"].iloc[0]
        except FileNotFoundError:
            pass

        # 새 토큰 발급 요청
        url = f"{self.real_domain}/oauth2/tokenP"
        headers = {"content-type": "application/json"}
        body = {"grant_type": "client_credentials", "appkey": self.appkey, "appsecret": self.appsecret}

        response = requests.post(url, headers=headers, data=json.dumps(body))
        if response.status_code == 200:
            access_token = response.json()["access_token"]
            pd.DataFrame([{
                "token_issue_date": today_date,
                f"{self.user_info['CANO']}_token": access_token
            }]).to_csv(csv_file_name, index=False, encoding="utf-8-sig")
            return access_token
        else:
            raise Exception(f"Failed to issue access token: {response.json()}")

    def _get_headers(self, access_token, tr_id):
        """
        공통 헤더 생성 메서드
        :param access_token: 액세스 토큰 (str)
        :param tr_id: 트랜잭션 ID (str)
        :return: API 요청 헤더 (dict)
        """
        return {
            "authorization": f"Bearer {access_token}",
            "appkey": self.appkey,
            "appsecret": self.appsecret,
            "tr_id": tr_id,
            "custtype": "P"
        }

    def _request(self, endpoint, headers, params=None, method="GET", body=None):
        """
        공통 API 요청 메서드
        :param endpoint: API 엔드포인트 (str)
        :param headers: 요청 헤더 (dict)
        :param params: GET 요청의 쿼리 매개변수 (dict)
        :param method: HTTP 메서드 ("GET" 또는 "POST")
        :param body: POST 요청의 본문 데이터 (dict)
        :return: API 응답 데이터 (dict)
        """
        url = f"{self.real_domain}{endpoint}"
        try:
            if method == "GET":
                response = requests.get(url, headers=headers, params=params)
            else:
                response = requests.post(url, headers=headers, data=json.dumps(body))
            
            # 요청 속도 제한 조절 (예: 초당 20건)
            time.sleep(0.05)  # 0.05초 지연 추가 (1초에 최대 20건)

            # 요청 성공 처리
            if response.json().get('rt_cd') == '0':
                return response.json()
            else:
                # 요청 실패 시 상태 코드 및 상세 응답 출력
                error_message = (
                    f"API Request Failed: {response.json().get('rt_cd')}\n"
                    f"Response Content: {response.content.decode('utf-8')}"
                )
                raise Exception(error_message)
        except Exception as e:
            print(f"Error occurred during API request: {e}")
            raise

    def rename_columns(self, df, column_map):
        """
        DataFrame 컬럼 이름 변경
        :param df: 원본 데이터프레임 (DataFrame)
        :param column_map: 컬럼 이름 매핑 (dict)
        :return: 컬럼명이 변경된 DataFrame
        """
        return df.rename(columns=column_map)

    def get_daily_price(self, FID_INPUT_ISCD, FID_COND_MRKT_DIV_CODE='J', FID_PERIOD_DIV_CODE='D', FID_ORG_ADJ_PRC='0'):
        """
        일별 주가 조회 API
        :param FID_INPUT_ISCD: 종목번호 (6자리)
        :param FID_COND_MRKT_DIV_CODE: 시장 구분 (기본값: J - 주식, ETF, ETN)
        :param FID_PERIOD_DIV_CODE: 기간 구분 (기본값: D - 일)
        :param FID_ORG_ADJ_PRC: 수정주가 여부 (기본값: 0 - 반영)
        :return: 일별 주가 데이터 (DataFrame)
        """
        access_token = self.issue_access_token()
        headers = self._get_headers(access_token, 'FHKST01010400')
        params = {
            "FID_COND_MRKT_DIV_CODE": FID_COND_MRKT_DIV_CODE,
            "FID_INPUT_ISCD": FID_INPUT_ISCD,
            "FID_PERIOD_DIV_CODE": FID_PERIOD_DIV_CODE,
            "FID_ORG_ADJ_PRC": FID_ORG_ADJ_PRC
        }

        response = self._request("/uapi/domestic-stock/v1/quotations/inquire-daily-price", headers, params=params)
        df = pd.DataFrame(response.get('output', []))
        return self.rename_columns(df, self.var_mapping)

    def get_index_daily_price(self, FID_INPUT_ISCD, FID_INPUT_DATE_1, FID_PERIOD_DIV_CODE='D', FID_COND_MRKT_DIV_CODE='U'):
        """
        업종 일별 지수 조회 API
        :param FID_INPUT_ISCD: 업종 코드 (예: '0001' - 코스피)
        :param FID_INPUT_DATE_1: 조회 시작 날짜 (YYYYMMDD)
        :param FID_PERIOD_DIV_CODE: 기간 구분 코드 (기본값: 'D' - 일별)
        :param FID_COND_MRKT_DIV_CODE: 시장 구분 코드
        :return: 일별 지수 데이터 (DataFrame)
        """
        access_token = self.issue_access_token()
        headers = self._get_headers(access_token, 'FHPUP02120000')
        params = {
            "FID_COND_MRKT_DIV_CODE": FID_COND_MRKT_DIV_CODE,
            "FID_INPUT_ISCD": FID_INPUT_ISCD,
            "FID_INPUT_DATE_1": FID_INPUT_DATE_1,
            "FID_PERIOD_DIV_CODE": FID_PERIOD_DIV_CODE
        }

        response = self._request("/uapi/domestic-stock/v1/quotations/inquire-index-daily-price", headers, params=params)
        return pd.DataFrame(response.get("output2", []))

    def get_index_category_price(self, FID_INPUT_ISCD='0001', FID_COND_MRKT_DIV_CODE="U", FID_COND_SCR_DIV_CODE='20214', FID_MRKT_CLS_CODE='K', FID_BLNG_CLS_CODE='0'):
        """
        업종별 지수 조회 API
        :param FID_INPUT_ISCD: 업종 코드 (예: '0001' - 코스피)
        :param FID_COND_MRKT_DIV_CODE: 시장 구분 코드 (기본값: 'U')
        :param FID_COND_SCR_DIV_CODE: 스크리닝 코드
        :param FID_MRKT_CLS_CODE: 시장 분류 코드
        :param FID_BLNG_CLS_CODE: 종목 포함 여부
        :return: 업종별 지수 데이터 (dict)
        """
        access_token = self.issue_access_token()
        headers = self._get_headers(access_token, 'FHPUP02140000')
        params = {
            "FID_COND_MRKT_DIV_CODE": FID_COND_MRKT_DIV_CODE,
            "FID_INPUT_ISCD": FID_INPUT_ISCD,
            "FID_COND_SCR_DIV_CODE": FID_COND_SCR_DIV_CODE,
            "FID_MRKT_CLS_CODE": FID_MRKT_CLS_CODE,
            "FID_BLNG_CLS_CODE": FID_BLNG_CLS_CODE
        }

        response = self._request("/uapi/domestic-stock/v1/quotations/inquire-daily-price", headers, params=params)
        return response

    def get_item_chart_price(self, FID_INPUT_ISCD, FID_INPUT_DATE_1, FID_INPUT_DATE_2, FID_COND_MRKT_DIV_CODE='J', FID_PERIOD_DIV_CODE='D', FID_ORG_ADJ_PRC='0'):
        """
        일별 종목 차트 조회 API
        :param FID_INPUT_ISCD: 종목 코드 (6자리)
        :param FID_INPUT_DATE_1: 조회 시작 날짜 (YYYYMMDD)
        :param FID_INPUT_DATE_2: 조회 종료 날짜 (YYYYMMDD)
        :param FID_COND_MRKT_DIV_CODE: 시장 구분 코드 (기본값: 'J')
        :param FID_PERIOD_DIV_CODE: 기간 구분 코드 (기본값: 'D')
        :param FID_ORG_ADJ_PRC: 수정주가 반영 여부 (기본값: '0')
        :return: 일별 종목 차트 데이터 (DataFrame)
        """
        access_token = self.issue_access_token()
        headers = self._get_headers(access_token, 'FHKST03010100')
        params = {
            "FID_COND_MRKT_DIV_CODE": FID_COND_MRKT_DIV_CODE,
            "FID_INPUT_ISCD": FID_INPUT_ISCD,
            "FID_INPUT_DATE_1": FID_INPUT_DATE_1,
            "FID_INPUT_DATE_2": FID_INPUT_DATE_2,
            "FID_PERIOD_DIV_CODE": FID_PERIOD_DIV_CODE,
            "FID_ORG_ADJ_PRC": FID_ORG_ADJ_PRC
        }

        response = self._request("/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", headers, params=params)
        df = pd.DataFrame(response.get("output2", []))
        return self.rename_columns(df, self.var_mapping)

    def search_stock_info(self, PDNO, PRDT_TYPE_CD='300'):
        """
        개별 종목 정보 조회 API
        :param PDNO: 종목번호 (6자리)
        :param PRDT_TYPE_CD: 상품 유형 코드 (기본값: '300')
        :return: 개별 종목 정보 (DataFrame)
        """
        access_token = self.issue_access_token()
        headers = self._get_headers(access_token, 'CTPF1002R')
        params = {"PRDT_TYPE_CD": PRDT_TYPE_CD, "PDNO": PDNO}

        response = self._request("/uapi/domestic-stock/v1/quotations/search-stock-info", headers, params=params)
        return pd.DataFrame([response.get("output", {})])

    def place_order(self, PDNO, CANO, ORD_DVSN, ORD_QTY, ORD_UNPR, order_type="buy", ACNT_PRDT_CD='01', tr_cont=''):
        """
        매수/매도 주문 API
        :param PDNO: 종목코드 (6자리)
        :param CANO: 계좌번호 앞 8자리
        :param ORD_DVSN: 주문 구분 (예: "01" - 시장가)
        :param ORD_QTY: 주문 수량
        :param ORD_UNPR: 주문 가격
        :param order_type: 주문 유형 ("buy" 또는 "sell")
        :param ACNT_PRDT_CD: 계좌 상품 코드 (기본값: "01")
        :param tr_cont: 연속 거래 여부
        :return: 매수/매도 주문 결과 (dict)
        """
        tr_id = "TTTC0802U" if order_type == "buy" else "TTTC0801U"
        access_token = self.issue_access_token()
        headers = self._get_headers(access_token, tr_id)
        body = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": PDNO,
            "ORD_DVSN": ORD_DVSN,
            "ORD_QTY": ORD_QTY,
            "ORD_UNPR": ORD_UNPR,
            "TR_CONT": tr_cont
        }

        response = self._request("/uapi/domestic-stock/v1/trading/order-cash", headers, body=body, method="POST")
        return response.get("output", {})
