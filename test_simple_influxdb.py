#!/usr/bin/env python3
"""
InfluxDB 연결 테스트 - 최소한의 단순한 버전
"""
from influxdb import InfluxDBClient


def test_basic_connection():
    """기본 연결 테스트"""
    print("=" * 50)
    print("InfluxDB 기본 연결 테스트")
    print("=" * 50)

    try:
        # 1. 클라이언트 생성
        print("1. InfluxDB 클라이언트 생성...")
        client = InfluxDBClient(
            host="10.238.27.132",
            port=8086,
            username="read_user",
            password="!Skepinfluxuser25",
            database="SRS1",
            timeout=30,
        )
        print("✅ 클라이언트 생성 완료")

        # 2. 연결 테스트
        print("\n2. 연결 테스트...")
        databases = client.get_list_database()
        print(
            f"✅ 연결 성공! 사용 가능한 데이터베이스: {[db['name'] for db in databases]}"
        )

        # 3. 현재 데이터베이스의 측정값 목록 조회
        print("\n3. 측정값 목록 조회...")
        measurements = client.get_list_measurements()
        print(f"✅ 측정값 목록: {[m['name'] for m in measurements]}")

        return client, measurements

    except Exception as e:
        print(f"❌ 연결 실패: {e}")
        return None, None


def test_simple_query(client, measurement_name="NOX_Value", limit=5):
    """간단한 쿼리 테스트"""
    print(f"\n" + "=" * 50)
    print(f"간단한 쿼리 테스트: {measurement_name}")
    print("=" * 50)

    try:
        # 쿼리 작성
        query = f"""
        SELECT *
        FROM "{measurement_name}"
        WHERE time > now() - 1h
        ORDER BY time DESC
        LIMIT {limit}
        """

        print(f"실행 쿼리: {query}")

        # 쿼리 실행
        result = client.query(query)

        # 결과 출력
        points = result.get_points()
        point_count = 0

        print(f"\n결과 (최근 {limit}개):")
        print("-" * 30)

        for point in points:
            point_count += 1
            print(f"{point_count}. 시간: {point['time']}")
            print(f"   값: {point.get('value', 'N/A')}")
            print(f"   전체 데이터: {point}")
            print()

        if point_count == 0:
            print("❌ 데이터가 없습니다.")
        else:
            print(f"✅ 총 {point_count}개의 데이터 포인트 조회 완료")

        return result

    except Exception as e:
        print(f"❌ 쿼리 실패: {e}")
        return None


def test_available_measurements(client):
    """사용 가능한 측정값들을 테스트"""
    print("\n" + "=" * 50)
    print("사용 가능한 측정값들 테스트")
    print("=" * 50)

    try:
        # 측정값 목록 조회
        measurements = client.get_list_measurements()
        measurement_names = [m["name"] for m in measurements]

        print(f"총 {len(measurement_names)}개의 측정값 발견:")
        for i, name in enumerate(measurement_names[:10], 1):  # 처음 10개만 출력
            print(f"{i:2d}. {name}")

        if len(measurement_names) > 10:
            print(f"... 외 {len(measurement_names) - 10}개 더")

        # NOx 관련 측정값 찾기
        nox_measurements = [name for name in measurement_names if "NOX" in name.upper()]
        if nox_measurements:
            print(f"\n🔍 NOx 관련 측정값: {nox_measurements}")
        else:
            print("\n🔍 NOx 관련 측정값을 찾을 수 없습니다.")

        return measurement_names

    except Exception as e:
        print(f"❌ 측정값 조회 실패: {e}")
        return []


if __name__ == "__main__":
    # 1. 기본 연결 테스트
    client, measurements = test_basic_connection()

    if client:
        # 2. 사용 가능한 측정값 확인
        measurement_names = test_available_measurements(client)

        # 3. 첫 번째 측정값으로 쿼리 테스트
        if measurement_names:
            first_measurement = measurement_names[0]
            print(f"\n첫 번째 측정값 '{first_measurement}'으로 쿼리 테스트:")
            test_simple_query(client, first_measurement, limit=3)

        # 4. NOx 관련 측정값이 있으면 테스트
        nox_measurements = [name for name in measurement_names if "NOX" in name.upper()]
        if nox_measurements:
            print(f"\nNOx 측정값 '{nox_measurements[0]}'으로 쿼리 테스트:")
            test_simple_query(client, nox_measurements[0], limit=3)

        # 5. 연결 종료
        client.close()
        print("\n✅ 모든 테스트 완료!")
    else:
        print("\n❌ 연결 실패로 테스트를 중단합니다.")
