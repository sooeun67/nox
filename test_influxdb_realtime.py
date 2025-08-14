import logging
import pandas as pd
import numpy as np
from data_preprocessor import NOxDataPreprocessor
import pickle
import os
import sys
from datetime import datetime, timedelta
from influxdb import InfluxDBClient

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class SRS1InfluxDBClient:
    """SRS1 InfluxDB 클라이언트"""

    def __init__(self):
        # 개발 InfluxDB 설정
        self.client = InfluxDBClient(
            host="10.238.24.150",
            port=8086,
            username="read_user",
            password="!Skepinfluxuser25",
            database="SRS1",
        )
        self.database = "SRS1"

    def _make_read_query(
        self,
        columns: list,
        start_time: pd.Timestamp,
        query_range_seconds: int,
        table_name: str = "SRS1",
    ) -> str:
        """시간 범위 기반 쿼리 생성"""
        end_time = start_time - pd.Timedelta(seconds=query_range_seconds)
        start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")

        query = f"""
            SELECT {",".join(columns)}
            FROM {table_name}
            WHERE time > '{end_time_str}' and time <= '{start_time_str}'
            ORDER BY time
        """
        return query

    def read_data(
        self,
        columns: list,
        start_time: pd.Timestamp,
        query_range_seconds: int,
        table_name: str = "SRS1",
    ) -> pd.DataFrame:
        """SRS1 데이터 조회"""
        try:
            query = self._make_read_query(
                columns, start_time, query_range_seconds, table_name
            )
            print(f"🔍 쿼리 실행: {query_range_seconds}초 범위")

            result_set = self.client.query(query)
            data = pd.DataFrame(result_set.get_points())

            if data.empty:
                print("⚠️ 조회된 데이터가 없습니다.")
                return pd.DataFrame()

            # 시간 컬럼을 _time_gateway로 변환
            if "time" in data.columns:
                data["_time_gateway"] = pd.to_datetime(data["time"])
                data = data.drop(columns=["time"])

            # 컬럼명을 소문자로 변환 (InfluxDB는 대문자, 우리 코드는 소문자)
            column_mapping = {}
            for col in data.columns:
                if col != "_time_gateway":
                    column_mapping[col] = col.lower()

            data = data.rename(columns=column_mapping)

            print(f"✅ 데이터 조회 완료: {data.shape}")
            print(f"   컬럼: {list(data.columns)}")
            print(
                f"   시간 범위: {data['_time_gateway'].min()} ~ {data['_time_gateway'].max()}"
            )

            return data

        except Exception as e:
            print(f"❌ 데이터 조회 실패: {e}")
            import traceback

            traceback.print_exc()
            return pd.DataFrame()


def test_influxdb_connection():
    """InfluxDB 연결 테스트"""
    print("🔌 InfluxDB 연결 테스트 시작")
    print("=" * 60)

    try:
        client = SRS1InfluxDBClient()

        # 간단한 쿼리로 연결 테스트
        test_query = "SHOW MEASUREMENTS"
        result = client.client.query(test_query)

        print("✅ InfluxDB 연결 성공")
        print(f"   사용 가능한 테이블: {list(result.get_points())}")
        return client

    except Exception as e:
        print(f"❌ InfluxDB 연결 실패: {e}")
        return None


def fetch_realtime_data(client: SRS1InfluxDBClient):
    """실시간 SRS1 데이터 조회"""
    print("📊 실시간 SRS1 데이터 조회 시작")
    print("=" * 60)

    try:
        # NOx 예측에 필요한 컬럼들 (대문자로 InfluxDB에서 조회)
        columns = [
            "BFT_EO_FG_T",
            "BR1_EO_FG_T",
            "BR1_EO_O2_A",
            "BR1_EO_ST_T",
            "DR1_EQ_BW_C",
            "ICF_CCS_FG_T_1",
            "ICF_CRA_WT_K",
            "ICF_FF1_AR_F_1",
            "ICF_FF1_SS_S_1",
            "ICF_FF1_SS_S_2",
            "ICF_FF2_SS_S_1",
            "ICF_IDF_SS_S_1",
            "ICF_SCS_FG_T_1",
            "ICF_TMS_NOX_A",
            "SDR_HTR_FG_T",
        ]

        # 현재 시간부터 1시간 전까지 데이터 조회
        end_time = pd.Timestamp.now()
        start_time = end_time - pd.Timedelta(hours=1)
        query_range_seconds = 3600  # 1시간

        print(f"⏰ 조회 기간: {start_time} ~ {end_time}")

        # 데이터 조회
        raw_data = client.read_data(
            columns=columns,
            start_time=end_time,
            query_range_seconds=query_range_seconds,
            table_name="SRS1",
        )

        if not raw_data.empty:
            print(f"✅ 데이터 조회 성공: {raw_data.shape}")
            return raw_data
        else:
            print("❌ 데이터 조회 실패")
            return None

    except Exception as e:
        print(f"❌ 데이터 조회 중 오류: {e}")
        import traceback

        traceback.print_exc()
        return None


def test_full_pipeline(client: SRS1InfluxDBClient):
    """전체 파이프라인 테스트"""
    print("🔄 전체 파이프라인 테스트 시작")
    print("=" * 60)

    # 1. SRS1 데이터 조회
    raw_data = fetch_realtime_data(client)
    if raw_data is None or raw_data.empty:
        print("❌ 데이터 조회 실패")
        return None

    # 2. 전처리 실행
    print("\n🔄 전처리 파이프라인 실행 중...")
    preprocessor = NOxDataPreprocessor()

    try:
        processed_data, feature_cols = preprocessor.preprocess_realtime_data(raw_data)
        print(f"✅ 전처리 완료: {processed_data.shape}")

        # 3. 모델 예측
        print("\n🤖 모델 예측 실행 중...")
        with open("Model/lgbm_model.pkl", "rb") as f:
            model = pickle.load(f)

        # 피처 매칭
        model_features = model.feature_names_in_
        available_features = [f for f in model_features if f in feature_cols]

        print(f"🔍 피처 매칭 결과:")
        print(f"   모델 피처 수: {len(model_features)}")
        print(f"   사용 가능한 피처: {len(available_features)}개")

        if len(available_features) > 0:
            model_input = processed_data[available_features].fillna(0)
            predictions = model.predict(model_input)

            print(f"✅ 예측 완료: {len(predictions)}개")
            print(f"   예측값 범위: {predictions.min():.2f} ~ {predictions.max():.2f}")
            print(f"   예측값 평균: {predictions.mean():.2f}")

            # 결과 저장
            result_df = processed_data.copy()
            result_df["nox_prediction"] = predictions

            print(f"\n📊 최종 결과: {result_df.shape}")
            return result_df
        else:
            print("❌ 사용 가능한 피처가 없습니다.")
            return None

    except Exception as e:
        print(f"❌ 파이프라인 실행 실패: {e}")
        import traceback

        traceback.print_exc()
        return None


if __name__ == "__main__":
    print("🚀 SRS1 InfluxDB 실시간 테스트 시작")
    print("=" * 60)

    # 1. InfluxDB 연결 테스트
    client = test_influxdb_connection()
    if client:
        # 2. 전체 파이프라인 테스트
        result = test_full_pipeline(client)

        if result is not None:
            print("\n🎊 모든 테스트 완료!")
            print("실시간 SRS1 InfluxDB 파이프라인이 정상 작동합니다.")
        else:
            print("\n⚠️ 파이프라인 테스트 실패")
    else:
        print("\n❌ InfluxDB 연결 실패")
