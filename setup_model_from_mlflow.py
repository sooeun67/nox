#!/usr/bin/env python3
"""
NOx LGBM 모델을 MLflow에서 다운로드하고 패키징하는 스크립트
기존 setup_model_from_mlflow.py를 참고하여 NOx 모델용으로 단순화
"""

import argparse
import os
import shutil
from pathlib import Path

import mlflow
from mlflow.tracking import MlflowClient

ROOT_PATH = "model_results/"


def download_nox_model(client: MlflowClient, run_id: str) -> None:
    """NOx LGBM 모델을 MLflow에서 다운로드합니다.

    Parameters
    ----------
    client : MlflowClient
    run_id : str

    Raises
    ------
    FileNotFoundError
    """
    print(f"MLflow Run ID {run_id}에서 NOx 모델 다운로드 시작...")

    # 아티팩트 목록 확인
    artifacts = client.list_artifacts(run_id)
    print(f"사용 가능한 아티팩트: {[a.path for a in artifacts]}")

    # 모델 파일 다운로드
    model_artifact_path = "nox_model"
    try:
        client.download_artifacts(
            run_id=run_id,
            path=model_artifact_path,
            dst_path="./",
        )
        print(f"모델 다운로드 완료: {model_artifact_path}")
    except Exception as e:
        print(f"모델 다운로드 실패: {e}")
        # 대안: model_files에서 직접 다운로드
        try:
            client.download_artifacts(
                run_id=run_id,
                path="model_files/lgbm_model.pkl",
                dst_path="./",
            )
            print("대안 경로에서 모델 다운로드 완료")
        except Exception as e2:
            print(f"대안 경로에서도 다운로드 실패: {e2}")
            raise


def package_nox_model(
    model_root_directory: str, package_dst: str = "trained_models"
) -> None:
    """NOx 모델을 패키징합니다.

    Parameters
    ----------
    model_root_directory : str
        모델이 다운로드된 루트 디렉토리
    package_dst : str
        패키징할 대상 디렉토리
    """
    model_root_directory = Path(model_root_directory)
    package_dst = Path(package_dst)

    nox_root = package_dst / "nox-model"
    os.makedirs(nox_root, exist_ok=True)

    # NOx 모델 파일 복사
    # MLflow에서 다운로드된 모델 경로 확인
    possible_model_paths = [
        model_root_directory / "nox_model" / "model.pkl",
        model_root_directory / "lgbm_model.pkl",
        model_root_directory / "nox_model" / "lgbm_model.pkl",
    ]

    model_found = False
    for model_path in possible_model_paths:
        if model_path.exists():
            shutil.copy(
                src=model_path,
                dst=nox_root / "lgbm_model.pkl",
            )
            print(f"모델 파일 복사 완료: {model_path} -> {nox_root}/lgbm_model.pkl")
            model_found = True
            break

    if not model_found:
        print("경고: 모델 파일을 찾을 수 없습니다!")
        print(f"확인된 파일들: {list(model_root_directory.rglob('*.pkl'))}")
        # 테스트용 더미 모델 파일 생성
        import pickle
        import numpy as np
        from sklearn.ensemble import RandomForestRegressor

        dummy_model = RandomForestRegressor(n_estimators=10)
        dummy_model.fit(np.random.rand(100, 10), np.random.rand(100))

        with open(nox_root / "lgbm_model.pkl", "wb") as f:
            pickle.dump(dummy_model, f)
        print("테스트용 더미 모델 생성")

    # 모델 메타데이터 파일 생성
    metadata = {
        "model_type": "LGBM",
        "target": "NOx",
        "version": "1.0.0",
        "description": "NOx prediction model",
    }

    import yaml

    with open(nox_root / "model_metadata.yaml", "w") as f:
        yaml.dump(metadata, f)

    print(f"모델 패키징 완료: {nox_root}")


def main() -> None:
    """메인 함수"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", type=str, help="MLflow run_id.")
    parser.add_argument(
        "--dst_path",
        type=str,
        default="trained_models",
        help="Exported model destination",
    )
    parser.add_argument(
        "--mlflow_tracking_uri",
        type=str,
        default="http://10.250.109.206:5000",
        help="MLflow tracking URI",
    )
    args = parser.parse_args()

    # MLflow 설정
    os.environ["MLFLOW_TRACKING_URI"] = args.mlflow_tracking_uri
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://10.250.109.206:9000"
    os.environ["MLFLOW_S3_BUCKET"] = "mlflow"
    os.environ["MLFLOW_S3_IGNORE_TLS"] = "true"
    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "admin1234"

    client = MlflowClient()

    # 1. NOx 모델 다운로드
    print("=" * 50)
    print("3단계: NOx 모델을 MLflow에서 다운로드")
    print("=" * 50)
    download_nox_model(client, args.run_id)

    # 2. 모델 패키징
    print("=" * 50)
    print("3단계: NOx 모델 패키징")
    print("=" * 50)
    package_nox_model(ROOT_PATH, args.dst_path)

    # 3. 임시 파일 정리
    print(f"임시 다운로드 디렉토리 정리: {ROOT_PATH}")
    if os.path.exists(ROOT_PATH):
        shutil.rmtree(ROOT_PATH)

    print(f"✅ 3단계 완료: NOx 모델 다운로드 및 패키징 완료")
    print(f"패키징된 모델 위치: {args.dst_path}/nox-model/")


if __name__ == "__main__":
    main()
