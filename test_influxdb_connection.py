#!/usr/bin/env python3
"""
InfluxDB 연결 테스트 및 실시간 데이터 조회
"""
import os
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
from datetime import datetime, timedelta

# InfluxDB 설정 (기존 aws 코드 참고)
INFLUXDB_HOST = "10.238.27.132"
INFLUXDB_PORT = "8086"
INFLUXDB_USERNAME = "read_user"
INFLUXDB_PASSWORD = "!Skepinfluxuser25"
INFLUXDB_DATABASE = "SRS1"  # 데이터베이스명
INFLUXDB_TIMEOUT = 30
INFLUXDB_BUCKET = "SRS1"


def test_influxdb_connection():
    """InfluxDB 연결을 테스트합니다."""
    print("=" * 50)
    print("InfluxDB 연결 테스트 시작")
    print("=" * 50)

    try:
        # InfluxDB 클라이언트 생성 (기존 aws 코드 방식)
        from influxdb import InfluxDBClient as LegacyInfluxDBClient

        client = LegacyInfluxDBClient(
            host=INFLUXDB_HOST,
            port=INFLUXDB_PORT,
            username=INFLUXDB_USERNAME,
            password=INFLUXDB_PASSWORD,
            database=INFLUXDB_DATABASE,
            timeout=INFLUXDB_TIMEOUT,
        )

        # 연결 테스트
        print("InfluxDB 연결 테스트...")

        # 데이터베이스 목록 조회
        databases = client.get_list_database()
        print(f"사용 가능한 데이터베이스: {[db['name'] for db in databases]}")

        # 현재 데이터베이스의 측정값 목록 조회
        measurements = client.get_list_measurements()
        print(f"현재 데이터베이스의 측정값: {[m['name'] for m in measurements]}")

        return client

    except Exception as e:
        print(f"InfluxDB 연결 실패: {e}")
        return None


def query_recent_data(client, measurement_name="nox", limit=10):
    """최근 데이터를 조회합니다."""
    print(f"\n{measurement_name} 측정값 최근 {limit}개 조회:")
    print("-" * 30)

    try:
        # 쿼리 작성
        # 기존 aws 코드 방식의 쿼리
        query = f"""
        SELECT *
        FROM "{measurement_name}"
        WHERE time > now() - 1h
        ORDER BY time DESC
        LIMIT {limit}
        """

        # 쿼리 실행
        result = client.query(query)

        if not result:
            print(f"'{measurement_name}' 측정값을 찾을 수 없습니다.")
            return None

        # 결과 출력 (기존 aws 코드 방식)
        points = result.get_points()
        for point in points:
            print(f"시간: {point['time']}, 데이터: {point}")

        return result

    except Exception as e:
        print(f"데이터 조회 실패: {e}")
        return None


def query_available_measurements(client):
    """사용 가능한 측정값들을 조회합니다."""
    print("\n사용 가능한 측정값 조회:")
    print("-" * 30)

    try:
        query = f"""
        import "influxdata/influxdb/schema"
        schema.measurements(bucket: "{INFLUXDB_BUCKET}")
        """

        query_api = client.query_api()
        result = query_api.query(query)

        measurements = []
        for table in result:
            for record in table.records:
                measurements.append(record.get_value())

        print(f"측정값 목록: {measurements}")
        return measurements

    except Exception as e:
        print(f"측정값 조회 실패: {e}")
        return []


def get_features_for_prediction(client, measurement_names, time_range="-5m"):
    """예측에 필요한 특성들을 조회합니다."""
    print(f"\n예측용 특성 데이터 조회 (최근 {time_range}):")
    print("-" * 30)

    features_data = {}

    for measurement in measurement_names:
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
                        features_data[measurement] = record.get_value()
                        print(f"{measurement}: {record.get_value()}")

        except Exception as e:
            print(f"{measurement} 조회 실패: {e}")

    return features_data


if __name__ == "__main__":
    # 1. 연결 테스트
    client = test_influxdb_connection()

    if client:
        # 2. 사용 가능한 측정값 조회
        measurements = query_available_measurements(client)

        # 3. 최근 데이터 조회 (예시)
        if measurements:
            # 처음 5개 측정값의 최근 데이터 조회
            for measurement in measurements[:5]:
                query_recent_data(client, measurement, limit=3)

        # 4. 예측용 특성 데이터 조회
        if measurements:
            features = get_features_for_prediction(client, measurements[:10])
            print(f"\n예측용 특성 데이터: {features}")

        client.close()
    else:
        print("InfluxDB 연결에 실패했습니다.")
