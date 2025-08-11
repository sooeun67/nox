#!/bin/bash
# NOx LGBM 모델용 export_model_to_ecr.sh
# 사용법:
# sudo bash export_model_to_ecr.sh <PLANT_CODE> <MLFLOW_RUN_ID> <GIT_TAG> <MINIO_ID> <MINIO_PW> <GIT_USER> <GIT_TOKEN>

# 예시:
# sudo bash export_model_to_ecr.sh srs1 <RUN_ID> v1.0.0 admin admin1234 root glpat-ZZmrz1nY4qPzUag2Pr3A

# ------------------------ 설정 영역 ------------------------
MLFLOW_TRACKING_URI=http://10.250.109.206:5000
MINIO_URI=http://10.250.109.206:9000

# ------------------------ 인자 처리 ------------------------
PLANT_CODE=$1
MLFLOW_RUN_ID=$2
GIT_TAG=$3
MINIO_ID=$4
MINIO_PW=$5
GIT_USER=$6
GIT_TOKEN=$7

# 인증 포함 GitLab Repo URL (NOx 모델용)
GIT_REPO="http://${GIT_USER}:${GIT_TOKEN}@10.250.109.206:8080/skep/srs1-urea-model.git"

# ------------------------ 출력 정보 ------------------------
echo ">>> NOx LGBM 모델 빌드 정보"
echo "PLANT_CODE: $PLANT_CODE"
echo "MLFLOW_RUN_ID: $MLFLOW_RUN_ID"
echo "GIT_TAG: $GIT_TAG"
echo "MLFLOW_TRACKING_URI: $MLFLOW_TRACKING_URI"
echo "MINIO_URI: $MINIO_URI"
echo "GIT_REPO: $GIT_REPO"
echo "Target arch: arm64"

# ------------------------ 도커 이미지 태그 ------------------------
IMG_TAG=035989641590.dkr.ecr.ap-northeast-2.amazonaws.com/zero4-wte-ai-makinarocks-dev-$PLANT_CODE-nox:$MLFLOW_RUN_ID-$GIT_TAG-arm64

# ------------------------ 도커 빌드 ------------------------
echo ">>> 3단계: NOx LGBM 모델 도커 이미지 빌드 시작"
echo "코드 버전: $GIT_TAG"
echo "MLflow Run ID: $MLFLOW_RUN_ID"

docker build \
  --platform linux/arm64 \
  --pull=false \
  --build-arg GIT_USER="$GIT_USER" \
  --build-arg GIT_TOKEN="$GIT_TOKEN" \
  --build-arg GIT_TAG="$GIT_TAG" \
  --build-arg MLFLOW_RUN_ID="$MLFLOW_RUN_ID" \
  --build-arg MLFLOW_TRACKING_URI="$MLFLOW_TRACKING_URI" \
  --build-arg MINIO_URI="$MINIO_URI" \
  --build-arg MINIO_ID="$MINIO_ID" \
  --build-arg MINIO_PW="$MINIO_PW" \
  -t "$IMG_TAG" \
  -f Dockerfile.lambda .

echo ">>> 3단계 완료: NOx LGBM 모델 도커 이미지 빌드 완료!"
echo "이미지 태그: $IMG_TAG"

# ------------------------ ECR 푸시는 필요시 주석 해제 ------------------------
# echo ">>> Login to AWS ECR..."
# aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin 035989641590.dkr.ecr.ap-northeast-2.amazonaws.com

# echo ">>> Push image to AWS ECR..."
# docker push $IMG_TAG
# echo ">>> ECR push completed!"

echo "✅ 모든 단계 완료!"
echo "이미지 태그: $IMG_TAG"
echo "다음 단계: ECR 푸시 또는 로컬 테스트" 