import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class NOxDataPreprocessor:
    """NOx 데이터 전처리 클래스"""

    def __init__(self):
        self.feature_cols = []
        self.logger = logging.getLogger(__name__)

    def preprocess_realtime_data(self, raw_data):
        """실시간 데이터 전처리 통합 파이프라인"""
        self.logger.info("🚀 NOx 데이터 전처리 시작")

        # 1단계: 기본 전처리
        data = self._basic_preprocessing(raw_data.copy())

        # 2단계: 폐기물 투입 피처
        data = self._create_trash_drop_features(data)

        # 3단계: 요약통계량 피처
        data, cols_x_stat = self._generate_interval_summary_features(data)

        # 4단계: 특수 피처
        data = self._mark_nox_spikes(data)

        # 5단계: 최종 피처 목록 생성
        self.feature_cols = self._create_final_feature_list(data, cols_x_stat)

        # 6단계: 모델 입력 준비
        model_data = self._prepare_model_input(data)

        self.logger.info("🎉 전처리 완료!")
        return model_data, self.feature_cols

    def _basic_preprocessing(self, data):
        """기본 데이터 정리"""
        self.logger.info("1️⃣ 기본 전처리")

        if "_time_gateway" in data.columns:
            data["_time_gateway"] = pd.to_datetime(data["_time_gateway"])
            # 시간 컬럼을 인덱스로 설정
            data = data.set_index("_time_gateway")
            self.logger.info("   ✅ 시간 컬럼 변환 및 인덱스 설정 완료")

        self.logger.info(f"   데이터 형태: {data.shape}")
        return data

    def _create_trash_drop_features(self, data):
        """폐기물 투입 피처 생성"""
        self.logger.info("2️⃣ 폐기물 투입 피처 생성")

        if "icf_cra_wt_k" not in data.columns:
            self.logger.warning(
                "   ⚠️ icf_cra_wt_k 컬럼이 없어 폐기물 투입 피처를 건너뜁니다."
            )
            data["trash_drop"] = 0
            data["trash_drop_count_30min"] = 0
            return data

        window_size_sec = 10
        diff_tolerance = -10

        data["trash_drop"] = (
            data["icf_cra_wt_k"].bfill().rolling(window_size_sec).max().diff()
            < diff_tolerance
        ).astype(int)

        data["trash_drop_count_30min"] = (
            data["trash_drop"].rolling("30min").sum().fillna(0)
        )

        self.logger.info("   ✅ 폐기물 투입 피처 생성 완료")
        return data

    def _generate_interval_summary_features(self, data):
        """요약통계량 피처 생성"""
        self.logger.info("3️⃣ 요약통계량 피처 생성")

        # 원본 변수 목록
        cols_x_original = [
            "bft_eo_fg_t",
            "br1_eo_fg_t",
            "br1_eo_o2_a",
            "br1_eo_st_t",
            "dr1_eq_bw_c",
            "icf_ccs_fg_t_1",
            "icf_cra_wt_k",
            "icf_ff1_ar_f_1",
            "icf_ff1_ss_s_1",
            "icf_ff1_ss_s_2",
            "icf_ff2_ss_s_1",
            "icf_idf_ss_s_1",
            "icf_scs_fg_t_1",
            "icf_tms_nox_a",
            "sdr_htr_fg_t",
            "trash_drop",
            "trash_drop_count_30min",
        ]

        interval_seconds = [60, 180, 300, 600, 1800]  # 1분, 3분, 5분, 10분, 30분
        new_columns = []

        for col in cols_x_original:
            if col in data.columns:
                self.logger.info(f"   🔄 {col} 처리 중...")
                for sec in interval_seconds:
                    window = pd.Timedelta(seconds=sec)

                    # 평균/표준편차
                    mean_col = f"{col}_mean_{sec}s"
                    std_col = f"{col}_std_{sec}s"
                    data[mean_col] = data.rolling(window=window)[col].mean()
                    data[std_col] = data.rolling(window=window)[col].std()
                    new_columns.extend([mean_col, std_col])

                    # 변화율/변화량
                    data_prev = data[[col]].copy()
                    data_prev_shifted = data_prev.copy()
                    data_prev_shifted.index += pd.Timedelta(seconds=sec)
                    data_prev_shifted.rename(columns={col: "_start_tmp"}, inplace=True)

                    data_matched = pd.merge_asof(
                        data,
                        data_prev_shifted,
                        left_index=True,
                        right_index=True,
                        direction="backward",
                        tolerance=pd.Timedelta(seconds=0),
                    )

                    start_val = data_matched["_start_tmp"].values
                    end_val = data[col].values

                    # 0으로 나누기 방지
                    start_val_safe = np.where(start_val == 0, 1e-10, start_val)

                    mean_rate_col = f"{col}_mean_rate_change_{sec}s"
                    range_change_col = f"{col}_range_change_{sec}s"
                    data[mean_rate_col] = (end_val - start_val) / start_val_safe
                    data[range_change_col] = end_val - start_val
                    new_columns.extend([mean_rate_col, range_change_col])

                    # 모멘텀
                    data["_val_diff"] = data[col].diff()
                    data["_time_diff"] = (
                        data.index.to_series().diff().dt.total_seconds()
                    )
                    data["_rate_per_sec"] = data["_val_diff"] / data[
                        "_time_diff"
                    ].replace(0, 1e-10)

                    momentum_up_col = f"{col}_momentum_max_up_{sec}s"
                    momentum_down_col = f"{col}_momentum_max_down_{sec}s"
                    data[momentum_up_col] = data.rolling(window=window)[
                        "_rate_per_sec"
                    ].max()
                    data[momentum_down_col] = data.rolling(window=window)[
                        "_rate_per_sec"
                    ].min()
                    new_columns.extend([momentum_up_col, momentum_down_col])

                    # 시작값 대비 최대 증가/감소량
                    rolling_window = data.rolling(window=window)[col]
                    max_inc_col = f"{col}_max_increase_from_start_{sec}s"
                    max_dec_col = f"{col}_max_decrease_from_start_{sec}s"
                    data[max_inc_col] = rolling_window.max() - start_val
                    data[max_dec_col] = rolling_window.min() - start_val
                    new_columns.extend([max_inc_col, max_dec_col])
            else:
                self.logger.warning(f"   ⚠️ {col} 컬럼이 데이터에 없습니다.")

        # 임시 컬럼 제거
        data.drop(
            columns=["_val_diff", "_time_diff", "_rate_per_sec"],
            inplace=True,
            errors="ignore",
        )

        self.logger.info(
            f"   ✅ 요약통계량 피처 생성 완료 - {len(new_columns)}개 컬럼 추가"
        )
        return data, new_columns

    def _mark_nox_spikes(self, data):
        """NOx 급등락 피처 생성"""
        self.logger.info("4️⃣ NOx 급등락 피처 생성")

        if "nox_value" not in data.columns:
            self.logger.warning(
                "   ⚠️ nox_value 컬럼이 없어 NOx 급등락 피처를 건너뜁니다."
            )
            data["nox_range_1min"] = 0
            data["nox_std_1min"] = 0
            data["is_spike"] = 0
            return data

        window_time_sec = 60
        spike_range_threshold = 8
        spike_std_threshold = 6

        window_time = pd.Timedelta(seconds=window_time_sec)
        data["nox_range_1min"] = (
            data.rolling(window=window_time)["nox_value"].max()
            - data.rolling(window=window_time)["nox_value"].min()
        )
        data["nox_std_1min"] = data.rolling(window=window_time)["nox_value"].std()

        data["is_spike"] = (
            (data["nox_range_1min"] > spike_range_threshold)
            & (data["nox_std_1min"] < spike_std_threshold)
        ).astype(int)

        spike_count = data["is_spike"].sum()
        self.logger.info(
            f"   ✅ 급등락 구간 탐지: {spike_count}개 ({spike_count/len(data)*100:.2f}%)"
        )

        return data

    def _create_final_feature_list(self, data, cols_x_stat):
        """최종 피처 목록 생성"""
        self.logger.info("5️⃣ 최종 피처 목록 생성")

        cols_x_original = [
            "bft_eo_fg_t",
            "br1_eo_fg_t",
            "br1_eo_o2_a",
            "br1_eo_st_t",
            "dr1_eq_bw_c",
            "icf_ccs_fg_t_1",
            "icf_cra_wt_k",
            "icf_ff1_ar_f_1",
            "icf_ff1_ss_s_1",
            "icf_ff1_ss_s_2",
            "icf_ff2_ss_s_1",
            "icf_idf_ss_s_1",
            "icf_scs_fg_t_1",
            "icf_tms_nox_a",
            "sdr_htr_fg_t",
            "trash_drop",
            "trash_drop_count_30min",
        ]

        feature_cols = ["is_spike"] + cols_x_original + cols_x_stat

        # 결측치가 많은 컬럼 제거
        na_count = data[feature_cols].isna().sum()
        cols_to_remove = na_count[na_count > 10000].index.tolist()
        feature_cols = [col for col in feature_cols if col not in cols_to_remove]

        self.logger.info(f"   ✅ 최종 피처 수: {len(feature_cols)}개")
        if cols_to_remove:
            self.logger.info(f"   🗑️ 제거된 피처: {len(cols_to_remove)}개")

        return feature_cols

    def _prepare_model_input(self, data):
        """모델 입력 준비"""
        self.logger.info("6️⃣ 모델 입력 준비")

        # 필요한 컬럼만 선택
        model_input_cols = self.feature_cols
        model_data = data[model_input_cols].copy()

        # 결측치가 있는 행 제거
        before_count = len(model_data)
        model_data = model_data.fillna(0)  # dropna() 대신 fillna(0) 사용
        after_count = len(model_data)

        self.logger.info(f"   📊 데이터 정리: {before_count:,} → {after_count:,} 행")
        self.logger.info(f"   🎯 최종 데이터 형태: {model_data.shape}")

        return model_data
