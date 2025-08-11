#!/usr/bin/env python3
"""
실시간 NOx 예측 스크립트
InfluxDB에서 실시간 데이터를 조회하고 학습된 모델로 NOx 예측을 수행합니다.
"""
import json
import logging
import pickle
import numpy as np
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient
from typing import Dict, List, Any

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# InfluxDB 설정
INFLUXDB_URL = "http://10.238.27.132:8086"
INFLUXDB_TOKEN = "read_user:실제비밀번호"  # 실제 비밀번호로 교체 필요
INFLUXDB_ORG = "srs1"
INFLUXDB_BUCKET = "srs1"

# NOx 예측에 필요한 측정값들 (실제 데이터에 맞게 수정 필요)
REQUIRED_MEASUREMENTS = [
    "temperature",  # 온도
    "pressure",  # 압력
    "flow_rate",  # 유량
    "oxygen_level",  # 산소 농도
    "fuel_rate",  # 연료 공급률
    "air_flow",  # 공기 유량
    "steam_flow",  # 증기 유량
    "combustion_temp",  # 연소 온도
    "exhaust_temp",  # 배기 가스 온도
    "nox_current",  # 현재 NOx 값 (이전 값)
]


def load_nox_model():
    """NOx LGBM 모델을 로드합니다."""
    try:
        model_path = "Model/lgbm_model.pkl"
        with open(model_path, "rb") as f:
            model = pickle.load(f)
        logger.info("NOx 모델 로드 완료")
        return model
    except Exception as e:
        logger.error(f"모델 로드 실패: {e}")
        raise


def get_influxdb_client():
    """InfluxDB 클라이언트를 생성합니다."""
    try:
        client = InfluxDBClient(
            url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG
        )

        # 연결 테스트
        health = client.health()
        logger.info(f"InfluxDB 연결 상태: {health}")

        return client
    except Exception as e:
        logger.error(f"InfluxDB 연결 실패: {e}")
        return None


def get_realtime_features(client, time_range="-5m"):
    """실시간 특성 데이터를 조회합니다."""
    logger.info(f"실시간 특성 데이터 조회 (최근 {time_range})")

    features = {}

    for measurement in REQUIRED_MEASUREMENTS:
        try:
            query = f"""
            from(bucket: "{INFLUXDB_BUCKET}")
                |> range(start: {time_range})
                |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                |> last()
            """

            query_api = client.query_api()
            result = query_api.query(query)

            if result:
                for table in result:
                    for record in table.records:
                        features[measurement] = record.get_value()
                        logger.info(f"{measurement}: {record.get_value()}")
            else:
                logger.warning(f"{measurement} 데이터를 찾을 수 없습니다.")
                features[measurement] = 0.0  # 기본값

        except Exception as e:
            logger.error(f"{measurement} 조회 실패: {e}")
            features[measurement] = 0.0  # 기본값

    return features


def prepare_features_for_prediction(features_dict):
    """특성 딕셔너리를 모델 입력 형식으로 변환합니다."""
    # REQUIRED_MEASUREMENTS 순서대로 특성 배열 생성
    feature_array = []
    for measurement in REQUIRED_MEASUREMENTS:
        if measurement in features_dict:
            feature_array.append(features_dict[measurement])
        else:
            feature_array.append(0.0)

    return np.array(feature_array).reshape(1, -1)


def predict_nox_realtime():
    """실시간 NOx 예측을 수행합니다."""
    logger.info("=" * 50)
    logger.info("실시간 NOx 예측 시작")
    logger.info("=" * 50)

    try:
        # 1. 모델 로드
        model = load_nox_model()

        # 2. InfluxDB 연결
        client = get_influxdb_client()
        if not client:
            raise Exception("InfluxDB 연결 실패")

        # 3. 실시간 특성 데이터 조회
        features_dict = get_realtime_features(client)

        # 4. 특성 데이터를 모델 입력 형식으로 변환
        features_array = prepare_features_for_prediction(features_dict)

        # 5. 예측 수행
        prediction = model.predict(features_array)[0]

        # 6. 결과 출력
        current_time = datetime.now()
        result = {
            "timestamp": current_time.isoformat(),
            "prediction": float(prediction),
            "features": features_dict,
            "feature_array": features_array.tolist()[0],
        }

        logger.info(f"예측 완료!")
        logger.info(f"시간: {current_time}")
        logger.info(f"NOx 예측값: {prediction:.4f}")
        logger.info(f"입력 특성: {features_dict}")

        return result

    except Exception as e:
        logger.error(f"실시간 예측 중 오류 발생: {e}")
        return None
    finally:
        if "client" in locals():
            client.close()


def continuous_prediction(interval_seconds=60, max_iterations=10):
    """지속적인 예측을 수행합니다."""
    logger.info(
        f"지속적 예측 시작 (간격: {interval_seconds}초, 최대: {max_iterations}회)"
    )

    import time

    for i in range(max_iterations):
        logger.info(f"\n--- 예측 #{i+1} ---")

        result = predict_nox_realtime()

        if result:
            print(f"✅ 예측 #{i+1} 성공: NOx = {result['prediction']:.4f}")
        else:
            print(f"❌ 예측 #{i+1} 실패")

        if i < max_iterations - 1:  # 마지막 반복이 아니면 대기
            logger.info(f"{interval_seconds}초 대기...")
            time.sleep(interval_seconds)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "continuous":
        # 지속적 예측 모드
        continuous_prediction()
    else:
        # 단일 예측 모드
        result = predict_nox_realtime()
        if result:
            print(f"\n🎯 최종 예측 결과:")
            print(f"시간: {result['timestamp']}")
            print(f"NOx 예측값: {result['prediction']:.4f}")
            print(f"입력 특성: {result['features']}")
        else:
            print("❌ 예측 실패")
