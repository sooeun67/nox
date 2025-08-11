# NOx LGBM 모델 배포 가이드

## 개요
이 프로젝트는 NOx 예측을 위한 LGBM 모델을 AWS Lambda에 배포하기 위한 코드입니다.

## 파일 구조
```
nox/
├── Model/
│   └── lgbm_model.pkl          # 학습된 LGBM 모델
├── upload_model_to_mlflow.py   # 1단계: MLflow 업로드
├── lambda_func.py              # 2단계: Lambda 함수
├── setup_model_from_mlflow.py  # 3단계: MLflow 다운로드
├── Dockerfile.lambda           # 3단계: Docker 이미지
├── export_model_to_ecr.sh      # 3단계: 빌드 스크립트
├── requirements.txt            # 의존성 패키지
└── README.md                   # 이 파일
```

## 단계별 배포 과정

### 1단계: MLflow에 모델 업로드
```bash
cd nox
python upload_model_to_mlflow.py
```
- `lgbm_model.pkl`을 MLflow에 업로드
- Run ID를 확인하고 기록

### 2단계: GitLab에 코드 업로드
1. GitLab에 `srs1-urea-model` 저장소 생성
2. 현재 nox 폴더의 내용을 업로드
3. 태그 생성 (예: v1.0.0)

### 3단계: 도커 이미지 빌드
```bash
cd nox
sudo bash export_model_to_ecr.sh <PLANT_CODE> <MLFLOW_RUN_ID> <GIT_TAG> <MINIO_ID> <MINIO_PW> <GIT_USER> <GIT_TOKEN>
```

예시:
```bash
sudo bash export_model_to_ecr.sh srs1 abc123def456 v1.0.0 admin admin1234 root glpat-ZZmrz1nY4qPzUag2Pr3A
```

## 테스트 방법

### 로컬 테스트
```bash
# 도커 이미지 실행
docker run -it <IMAGE_TAG> python -c "import lambda_func; print('Hello from NOx Lambda function!')"
```

### Lambda 함수 테스트
```bash
# 테스트 이벤트
{
  "features": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
}
```

## 주의사항
- 기본 이미지 `mrx-base:v2`에 LGBM 패키지가 없을 수 있음
- Dockerfile에서 필요한 패키지를 설치하도록 설정됨
- GitLab 인증 토큰이 필요함
- MLflow와 MinIO 접근 권한이 필요함 