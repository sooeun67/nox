import logging
import pandas as pd
import numpy as np
from data_preprocessor import NOxDataPreprocessor
import os

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def load_real_data():
    """실제 parquet 데이터에서 특정 기간 로드"""
    print("📊 실제 parquet 데이터 로드 중...")

    try:
        # parquet 파일 경로
        file_path = "Data/cleaned_240411_250724.parquet"

        # 파일 존재 확인
        if not os.path.exists(file_path):
            print(f"❌ 파일을 찾을 수 없습니다: {file_path}")
            return None

        print(f"전처리 대상 파일: {file_path}")

        # 특정 기간 데이터만 로드 (메모리 효율성)
        start_date = "2025-07-01"
        end_date = "2025-07-10"

        print(f"전처리 대상 데이터 기간: {start_date} ~ {end_date}")

        # parquet 파일에서 데이터 로드
        data = pd.read_parquet(file_path)
        print(f"   📊 전체 데이터: {data.shape}")

        # _time_gateway 컬럼을 datetime으로 변환
        if "_time_gateway" in data.columns:
            data["_time_gateway"] = pd.to_datetime(data["_time_gateway"])

            # 특정 기간 필터링
            mask = (data["_time_gateway"] >= start_date) & (
                data["_time_gateway"] <= end_date
            )
            data = data[mask].copy()

            print(f"✅ 필터링 완료: {data.shape}")
            print(
                f"📅 시간 범위: {data['_time_gateway'].min()} ~ {data['_time_gateway'].max()}"
            )

            # 메모리 정리
            data = data.reset_index(drop=True)

            return data
        else:
            print("❌ _time_gateway 컬럼을 찾을 수 없습니다.")
            return None

    except Exception as e:
        print(f"❌ 데이터 로드 중 오류 발생: {e}")
        import traceback

        traceback.print_exc()
        return None


def test_preprocessing():
    """전처리 파이프라인 테스트"""
    print("🧪 NOx 전처리 파이프라인 테스트 시작")
    print("=" * 60)

    # 1. 실제 데이터 로드
    sample_data = load_real_data()

    if sample_data is None:
        print("❌ 데이터 로드를 실패했습니다.")
        return None, None

    # 2. 전처리 실행
    print("\n🚀 전처리 파이프라인 실행 중...")
    preprocessor = NOxDataPreprocessor()

    try:
        processed_data, feature_cols = preprocessor.preprocess_realtime_data(
            sample_data
        )

        print("\n✅ 전처리 완료!")
        print("=" * 60)
        print(f"📊 최종 데이터 형태: {processed_data.shape}")
        print(f"🔢 피처 수: {len(feature_cols)}")
        print(f"📋 피처 목록 (처음 15개):")
        for i, feature in enumerate(feature_cols[:15]):
            print(f"   {i+1:2d}. {feature}")

        if len(feature_cols) > 15:
            print(f"   ... 외 {len(feature_cols) - 15}개")

        # 3. 데이터 품질 확인
        print("\n🔍 데이터 품질 확인:")
        print(f"결측치: {processed_data.isna().sum().sum()}개")
        print(
            f"무한값: {np.isinf(processed_data.select_dtypes(include=[np.number])).sum().sum()}개"
        )
        print(
            f"음의 무한값: {np.isneginf(processed_data.select_dtypes(include=[np.number])).sum().sum()}개"
        )

        # 4. 피처별 통계 정보
        print("\n📈 피처별 기본 통계:")
        numeric_cols = processed_data.select_dtypes(include=[np.number]).columns
        stats_df = processed_data[numeric_cols].describe()
        print(stats_df.round(3))

        return processed_data, feature_cols

    except Exception as e:
        print(f"\n❌ 전처리 중 오류 발생: {e}")
        import traceback

        traceback.print_exc()
        return None, None


def test_with_real_model(processed_data, feature_cols):
    """실제 모델과 함께 테스트 - 피처 매칭 적용"""
    print("\n🤖 실제 모델과 함께 테스트")
    print("=" * 60)

    try:
        # 모델 로드
        import pickle

        with open("Model/lgbm_model.pkl", "rb") as f:
            model = pickle.load(f)

        print(f"✅ 모델 로드 완료: {type(model).__name__}")

        # 모델이 기대하는 피처 확인
        model_features = model.feature_names_in_
        print(f"   모델 피처 수: {len(model_features)}")
        print(f"   전처리 피처 수: {len(feature_cols)}")

        # 사용 가능한 피처 찾기
        available_features = [f for f in model_features if f in feature_cols]
        missing_features = [f for f in model_features if f not in feature_cols]

        print(f"\n🔍 피처 매칭 결과:")
        print(f"   사용 가능한 피처: {len(available_features)}개")
        print(f"   누락된 피처: {len(missing_features)}개")

        if missing_features:
            print(f"   ⚠️ 누락된 피처 (처음 10개): {missing_features[:10]}")
            if len(missing_features) > 10:
                print(f"   ... 외 {len(missing_features) - 10}개")

        if len(available_features) > 0:
            # 사용 가능한 피처만으로 예측
            model_input = processed_data[available_features].fillna(0)

            print(f"\n🚀 예측 실행 중...")
            print(f"   입력 데이터 형태: {model_input.shape}")

            # 예측 실행
            predictions = model.predict(model_input)

            print(f"\n🔮 예측 완료!")
            print(f"   예측 데이터 수: {len(predictions)}")
            print(f"   예측값 범위: {predictions.min():.2f} ~ {predictions.max():.2f}")
            print(f"   예측값 평균: {predictions.mean():.2f}")

            # 결과를 데이터프레임에 추가
            result_df = processed_data.copy()
            result_df["nox_prediction"] = predictions

            print(f"\n📊 최종 결과 데이터:")
            print(f"   데이터 형태: {result_df.shape}")
            print(f"   컬럼 수: {len(result_df.columns)}")

            return result_df
        else:
            print("❌ 사용 가능한 피처가 없습니다.")
            return None

    except Exception as e:
        print(f"❌ 모델 테스트 중 오류 발생: {e}")
        import traceback

        traceback.print_exc()

    return None


if __name__ == "__main__":
    print("🚀 NOx 전처리 테스트 시작")
    print("=" * 60)

    # 1단계: 기본 전처리 테스트 (1회만 실행)
    processed_data, feature_cols = test_preprocessing()

    if processed_data is not None:
        print("\n🎉 기본 전처리 테스트 성공!")

        # 2단계: 실제 모델과 함께 테스트 (전처리 결과 재사용)
        print("\n" + "=" * 60)
        result_df = test_with_real_model(
            processed_data, feature_cols
        )  # 전처리된 데이터 전달

        if result_df is not None:
            print("\n🎊 모든 테스트 완료!")
            print("전처리 파이프라인이 정상적으로 작동합니다.")
        else:
            print("\n⚠️ 모델 테스트는 건너뛰었지만, 전처리는 성공했습니다.")
    else:
        print("\n❌ 전처리 테스트 실패")
        print("코드를 확인하고 다시 시도해주세요.")
