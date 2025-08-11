#!/usr/bin/env python3
"""
NOx LGBM 모델을 MLflow에 업로드하는 스크립트
1단계: 학습된 lgbm_model.pkl을 MLFlow에 보내서 RUN_ID를 생성합니다.
"""

import os
import pickle
import mlflow
import mlflow.sklearn
from datetime import datetime

# MLflow 설정
MLFLOW_TRACKING_URI = "http://10.250.109.206:5000"
MLFLOW_S3_ENDPOINT_URL = "http://10.250.109.206:9000"
MLFLOW_S3_BUCKET = "mlflow"
MLFLOW_S3_IGNORE_TLS = True


def upload_nox_model():
    """NOx LGBM 모델을 MLflow에 업로드"""
    print("=" * 50)
    print("1단계: NOx LGBM 모델을 MLflow에 업로드 시작")
    print("=" * 50)

    # MLflow 설정
    os.environ["MLFLOW_TRACKING_URI"] = MLFLOW_TRACKING_URI
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = MLFLOW_S3_ENDPOINT_URL
    os.environ["MLFLOW_S3_BUCKET"] = MLFLOW_S3_BUCKET
    os.environ["MLFLOW_S3_IGNORE_TLS"] = str(MLFLOW_S3_IGNORE_TLS)
    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "admin1234"

    # 모델 파일 경로
    model_path = "Model/lgbm_model.pkl"

    if not os.path.exists(model_path):
        raise FileNotFoundError(f"모델 파일을 찾을 수 없습니다: {model_path}")

    print(f"모델 파일 확인: {model_path}")

    # 모델 로드
    with open(model_path, "rb") as f:
        model = pickle.load(f)

    print("모델 로드 완료")

    # MLflow 실험 설정
    experiment_name = "nox-lgbm-model"
    mlflow.set_experiment(experiment_name)

    # 모델 업로드
    with mlflow.start_run() as run:
        print(f"MLflow Run 시작: {run.info.run_id}")

        # 모델 로깅
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="nox_model",
            registered_model_name="nox-lgbm-predictor",
        )

        # 모델 메타데이터 로깅
        mlflow.log_param("model_type", "LGBM")
        mlflow.log_param("target", "NOx")
        mlflow.log_param("upload_time", datetime.now().isoformat())

        # 모델 파일도 아티팩트로 저장
        mlflow.log_artifact(model_path, "model_files")

        print(f"모델 업로드 완료!")
        print(f"Run ID: {run.info.run_id}")
        print(f"실험 이름: {experiment_name}")
        print(f"모델 이름: nox-lgbm-predictor")

        return run.info.run_id


if __name__ == "__main__":
    try:
        run_id = upload_nox_model()
        print(f"\n✅ 1단계 완료: MLflow Run ID = {run_id}")
        print("이 Run ID를 다음 단계에서 사용하세요!")
    except Exception as e:
        print(f"❌ 1단계 실패: {e}")
        raise
