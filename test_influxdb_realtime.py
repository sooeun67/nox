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
            "_time_gateway",
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
            "NOX_Value",
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

    # 1. 실시간 데이터 조회
    raw_data = fetch_realtime_data(client)
    if raw_data is None or raw_data.empty:
        print("❌ 데이터 조회 실패")
        return None

    print(f"\n📊 원본 데이터 정보:")
    print(f"   데이터 형태: {raw_data.shape}")
    print(f"   컬럼 수: {len(raw_data.columns)}")
    print(
        f"   시간 범위: {raw_data['_time_gateway'].min()} ~ {raw_data['_time_gateway'].max()}"
    )
    print(f"   샘플 컬럼: {list(raw_data.columns[:10])}")

    # 2. 전처리 파이프라인 실행
    print("\n🔄 전처리 파이프라인 실행 중...")
    preprocessor = NOxDataPreprocessor()
    try:
        processed_data, feature_cols = preprocessor.preprocess_realtime_data(raw_data)
        print(f"✅ 전처리 완료: {processed_data.shape}")
        print(f"   생성된 피처 수: {len(feature_cols)}")

        # 전처리 후 컬럼 확인 - 더 자세한 정보 출력
        print(f"\n🔍 전처리 후 컬럼 확인:")
        print(f"   전체 컬럼 수: {len(processed_data.columns)}")
        print(
            f"   NOx 관련 컬럼: {[col for col in processed_data.columns if 'nox' in col.lower()]}"
        )
        print(
            f"   시간 관련 컬럼: {[col for col in processed_data.columns if 'time' in col.lower()]}"
        )

        # 원본 NOx 컬럼 확인
        print(
            f"   원본 NOx 컬럼 (nox_value): {[col for col in processed_data.columns if col == 'nox_value']}"
        )
        print(
            f"   icf_tms_nox_a 관련 컬럼: {[col for col in processed_data.columns if 'icf_tms_nox_a' in col][:5]}"
        )

        # 전체 컬럼에서 'nox'가 포함된 컬럼 상세 확인
        nox_columns = [col for col in processed_data.columns if "nox" in col.lower()]
        print(f"   NOx 관련 컬럼 상세:")
        for i, col in enumerate(nox_columns[:10]):  # 처음 10개만 출력
            print(f"     {i+1:2d}. {col}")
        if len(nox_columns) > 10:
            print(f"     ... 외 {len(nox_columns) - 10}개")

        # 원본 데이터에서 NOx 관련 컬럼 확인
        print(f"\n🔍 원본 데이터 NOx 컬럼 확인:")
        print(f"   원본 컬럼: {list(raw_data.columns)}")
        print(
            f"   NOx 관련 원본 컬럼: {[col for col in raw_data.columns if 'nox' in col.lower()]}"
        )

        # 전처리 전후 컬럼 비교
        print(f"\n🔍 전처리 전후 컬럼 비교:")
        print(
            f"   전처리 전 NOx 컬럼: {[col for col in raw_data.columns if 'nox' in col.lower()]}"
        )
        print(
            f"   전처리 후 NOx 컬럼: {[col for col in processed_data.columns if 'nox' in col.lower()]}"
        )

        # 3. 모델 예측 실행
        print("\n🤖 모델 예측 실행 중...")
        with open("Model/lgbm_model.pkl", "rb") as f:
            model = pickle.load(f)

        model_features = model.feature_names_in_
        available_features = [f for f in model_features if f in feature_cols]
        missing_features = [f for f in model_features if f not in feature_cols]

        print(f"🔍 피처 매칭 결과:")
        print(f"   모델 피처 수: {len(model_features)}")
        print(f"   사용 가능한 피처: {len(available_features)}개")
        print(f"   누락된 피처: {len(missing_features)}개")

        if missing_features:
            print(f"   ⚠️ 누락된 피처 (처음 10개): {missing_features[:10]}")
            if len(missing_features) > 10:
                print(f"   ... 외 {len(missing_features) - 10}개")

        if len(available_features) > 0:
            model_input = processed_data[available_features].fillna(0)

            print(f"\n🚀 예측 실행 중...")
            print(f"   입력 데이터 형태: {model_input.shape}")

            predictions = model.predict(model_input)

            print(f"✅ 예측 완료: {len(predictions)}개")
            print(f"   예측값 범위: {predictions.min():.2f} ~ {predictions.max():.2f}")
            print(f"   예측값 평균: {predictions.mean():.2f}")
            print(f"   예측값 표준편차: {predictions.std():.2f}")

            # 4. 결과 데이터프레임 생성
            result_df = processed_data.copy()
            result_df["nox_prediction"] = predictions

            # 인덱스를 컬럼으로 복원 (중요!)
            if result_df.index.name == "_time_gateway":
                result_df = result_df.reset_index()
                print("✅ _time_gateway 인덱스를 컬럼으로 복원")

            # NOX_Value를 별도로 추가 (Y값으로 사용)
            if "nox_value" in raw_data.columns:
                result_df["nox_value"] = raw_data["nox_value"]
                print("✅ nox_value (Y값) 추가")

            # 5. 필수 컬럼 확인 및 출력
            print(f"\n📊 최종 결과 데이터:")
            print(f"   데이터 형태: {result_df.shape}")
            print(f"   컬럼 수: {len(result_df.columns)}")

            # 필수 컬럼 확인
            required_columns = [
                "_time_gateway",
                "nox_value",
                "nox_prediction",
                "br1_eo_o2_a",
                "icf_scs_fg_t_1",
                "icf_ccs_fg_t_1",
            ]

            available_required = [
                col for col in required_columns if col in result_df.columns
            ]
            missing_required = [
                col for col in required_columns if col not in result_df.columns
            ]

            print(f"\n🔍 필수 컬럼 확인:")
            print(f"   사용 가능한 컬럼: {available_required}")
            if missing_required:
                print(f"   ⚠️ 누락된 컬럼: {missing_required}")

            # 6. 마지막 10개 행 출력
            if len(available_required) > 0:
                print(f"\n📋 마지막 10개 행 (필수 컬럼):")
                print("=" * 80)

                # 마지막 10개 행 선택
                last_10_rows = result_df[available_required].tail(10)

                # 시간 포맷팅
                if "_time_gateway" in last_10_rows.columns:
                    last_10_rows["_time_gateway"] = last_10_rows[
                        "_time_gateway"
                    ].dt.strftime("%Y-%m-%d %H:%M:%S")

                # 데이터 출력
                pd.set_option("display.max_columns", None)
                pd.set_option("display.width", None)
                print(last_10_rows.to_string(index=False))

                # 7. 통계 요약
                print(f"\n📈 예측 결과 통계:")
                print(f"   예측값 평균: {result_df['nox_prediction'].mean():.2f}")
                print(f"   예측값 중앙값: {result_df['nox_prediction'].median():.2f}")
                print(f"   예측값 표준편차: {result_df['nox_prediction'].std():.2f}")

                if "nox_value" in result_df.columns:
                    print(f"\n🔍 실제값 vs 예측값 비교:")
                    print(f"   실제값 평균: {result_df['nox_value'].mean():.2f}")
                    print(f"   예측값 평균: {result_df['nox_prediction'].mean():.2f}")
                    print(
                        f"   차이 평균: {abs(result_df['nox_value'] - result_df['nox_prediction']).mean():.2f}"
                    )

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
