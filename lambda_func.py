#!/usr/bin/env python3
"""
NOx 예측을 위한 Lambda 함수
2단계: Lambda 호출용 함수가 포함된 코드를 gitlab에 마련합니다.
"""

import json
import logging
import pickle
import os
from typing import Any, Dict
import numpy as np

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_nox_model():
    """NOx LGBM 모델을 로드합니다."""
    try:
        model_path = "trained_models/nox-model/lgbm_model.pkl"
        with open(model_path, "rb") as f:
            model = pickle.load(f)
        logger.info("NOx 모델 로드 완료")
        return model
    except Exception as e:
        logger.error(f"모델 로드 실패: {e}")
        raise


def nox_pred(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    NOx 예측을 수행하는 Lambda 함수

    Parameters
    ----------
    event : Dict[str, Any]
        입력 데이터 (예측에 필요한 특성들)
    context : Any
        Lambda 컨텍스트

    Returns
    -------
    Dict[str, Any]
        예측 결과
    """
    logger.info("=" * 20 + " Start NOx Prediction " + "=" * 20)
    logger.info(f"Event JSON: {event}")

    try:
        # 2단계 테스트: 연결 확인 메시지
        print("Hello from NOx Lambda function!")
        logger.info("Hello from NOx Lambda function!")

        # 모델 로드
        model = load_nox_model()

        # 입력 데이터 처리 (예시)
        # 실제로는 event에서 필요한 특성들을 추출해야 함
        if "features" in event:
            features = np.array(event["features"]).reshape(1, -1)
        else:
            # 테스트용 더미 데이터
            features = np.random.rand(1, 10)  # 10개 특성으로 가정
            logger.info("테스트용 더미 데이터 사용")

        # 예측 수행
        prediction = model.predict(features)[0]

        result = {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "NOx prediction completed",
                    "prediction": float(prediction),
                    "input_features": (
                        features.tolist()[0] if "features" in event else "dummy_data"
                    ),
                }
            ),
        }

        logger.info(f"예측 완료: {prediction}")
        return result

    except Exception as e:
        logger.error(f"예측 중 오류 발생: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Prediction failed: {str(e)}"}),
        }


def test_connection(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    연결 테스트용 함수
    """
    logger.info("=" * 20 + " Connection Test " + "=" * 20)
    print("Hello from NOx test function!")

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "NOx Lambda function connection test successful",
                "timestamp": "2025-01-27",
            }
        ),
    }
