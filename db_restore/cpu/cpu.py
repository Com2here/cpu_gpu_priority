import re
import pandas as pd
import json
import mysql.connector
from mysql.connector import Error
from pcpartpicker import API
from datetime import datetime

# ─── 1. 모델명 정규화 ─────────────────────────────────────────────
def normalize_cpu_model(name: str) -> str:
    if not name:
        return ""

    name = name.strip()

    # 한글 모델명을 영문으로 변환
    replacements = {
        "코어 울트라": "Core Ultra",
        "코어": "Intel Core ",
        "라이젠": "AMD Ryzen ",
        "펜티엄 골드 ": "Intel Pentium Gold ",
        "애슬론 ": "AMD Athlon ",
        "셀러론 ": "Intel Celeron ",
    }
    for kr, en in replacements.items():
        name = name.replace(kr, en)

    # AMD A107700K 같은 경우 → AMD A10-7700K로 바꾸기
    name = re.sub(r'\b(A\d{2})(\d{4})([A-Z]*)\b', r'\1-\2\3', name, flags=re.IGNORECASE)
    name = re.sub(r'\b(FX)(\d{4})([A-Z]*)\b', r'\1-\2\3', name, flags=re.IGNORECASE)
    name = re.sub(r'\b(i[3579])(\d{4})([A-Z]*)\b', r'\1-\2\3', name, flags=re.IGNORECASE)

    # 특수문자 중 하이픈은 유지하고 나머지만 제거
    name = re.sub(r'[^\w\s\-]', '', name)

    # 다중 공백 제거
    name = re.sub(r'\s+', ' ', name)

    return name.strip()

# ─── 2. 제외 대상 필터링 ─────────────────────────────────────────
def is_excludable_cpu_model(model):
    model = model.lower()
    return any(keyword in model for keyword in ["xeon", "epyc", "platinum", "opteron"])

# ─── 3. JSON 모델 로드 ───────────────────────────────────────────
def load_json_cpu_models(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    models_dict = {}  # normalized_name → detailed_info

    for item in data:
        model = item.get("name", "").strip()  # ← "model" → "name"
        if model and not is_excludable_cpu_model(model):
            normalized = normalize_cpu_model(model)

            detailed_info = {
                'model': model,
                'normalized_model': normalized,
                'cores': item.get('core_count'),
                'threads': item.get('core_count') * 2 if item.get('smt') else item.get('core_count'),
                'base_clock': item.get('core_clock'),
                'boost_clock': item.get('boost_clock'),
                'tdp': item.get('tdp'),
                'graphics': item.get('graphics'),
                'smt': item.get('smt'),
                'price': item.get('price')
            }

            models_dict[normalized] = detailed_info

    return models_dict

# ─── . JSON 모델 로드 (API 전용) ───────────────────────────────────────────
def load_api_cpu_models(region="us"):
    try:
        api = API(region)
        cpu_data = api.retrieve("cpu")
        cpu_dict = json.loads(cpu_data.to_json())
        raw_list = cpu_dict.get("cpu", [])

        print(f"📊 API에서 수신한 CPU 수: {len(raw_list)}")

        models_dict = {}
        sample_printed = 0

        for item in raw_list:
            brand = item.get("brand", "").strip()
            model = item.get("model", "").strip()
            if not brand or not model:
                print(f"❌ 누락: brand/model 없음 → {item}")
                continue

            full_name = f"{brand} {model}"

            # if sample_printed < 10:
            #     print(f"\n🧪 API 모델 샘플 {sample_printed + 1}:")
            #     for k, v in item.items():
            #         print(f"  {k}: {v}")
            #     sample_printed += 1

            if is_excludable_cpu_model(full_name):
                print(f"🚫 제외된 모델: {full_name}")
                continue

            normalized = normalize_cpu_model(full_name)

            detailed_info = {
                "model": full_name,
                "normalized_model": normalized,
                "cores": item.get("cores"),
                "threads": item.get("cores") * 2 if item.get("multithreading") else item.get("cores"),
                "base_clock": round(item.get("base_clock", {}).get("cycles", 0) / 1e9, 2) if item.get("base_clock") else None,
                "boost_clock": round(item.get("boost_clock", {}).get("cycles", 0) / 1e9, 2) if item.get("boost_clock") else None,
                "tdp": item.get("tdp"),
                "graphics": item.get("integrated_graphics"),
                "smt": item.get("multithreading"),
                "price": float(item.get("price", [None, 0])[1]) if item.get("price") else None,
                "source": "api"
            }

            if normalized in models_dict:
                existing = models_dict[normalized]
                if existing == detailed_info:
                    continue  # 내용이 완전히 동일하면 건너뜀
                else:
                    print(f"🔁 중복된 정규화 이름: {normalized} (기존: {existing['model']}, 새 모델: {full_name})")

            models_dict[normalized] = detailed_info

        print(f"\n🌐 정규화된 API CPU 모델 수: {len(models_dict)}")
        return models_dict

    except Exception as e:
        print(f"❌ API CPU 불러오기 실패: {e}")
        return {}



# ─── 4. 엑셀 → 정규화 후보군 추출 ────────────────────────────────
def create_cpu_variants(df, first_column):
    variants = []
    excluded = []

    for value in df[first_column]:
        if pd.notna(value):
            original = str(value).strip()
            if original == "게임 옵션" or "라인" in original:
                excluded.append(original)
                continue

            normalized = normalize_cpu_model(original)
            variants.append({
                "original": original,
                "normalized_name": normalized
            })

    # 정규화된 이름 10개 출력
    print("\n🧪 정규화된 CPU 이름 샘플 (최대 10개):")
    for v in variants[:30]:
        print(f"- {v['original']} → {v['normalized_name']}")

    return variants, excluded


# ─── 5. 조건별 매칭 수행 ────────────────────────────────────────
# def match_cpu_variants(variants, json_models_dict, api_models_dict):
#     matched = []
#     unmatched = []
#     seen = set()

#     for v in variants:
#         orig = v["original"]
#         norm = v["normalized_name"]

#         if orig in seen:
#             continue

#         if norm in json_models_dict:
#             matched.append({
#                 "original": orig,
#                 "normalized_name": norm,
#                 "cpu_details": json_models_dict[norm]
#             })
#         elif norm in api_models_dict:
#             matched.append({
#                 "original": orig,
#                 "normalized_name": norm,
#                 "cpu_details": api_models_dict[norm]
#             })
#         else:
#             unmatched.append(orig)

#         seen.add(orig)

#     return matched, unmatched
def match_cpu_variants(variants, json_models_dict, api_models_dict):
    matched = []
    unmatched = []
    seen = set()

    for v in variants:
        orig = v["original"]
        norm = v["normalized_name"]

        if orig in seen:
            continue

        if norm in json_models_dict:
            matched.append({
                "original": orig,
                "normalized_name": norm,
                "cpu_details": json_models_dict[norm]
            })
        elif norm in api_models_dict:
            matched.append({
                "original": orig,
                "normalized_name": norm,
                "cpu_details": api_models_dict[norm]
            })
        else:
            unmatched.append((orig, norm))  # ⬅️ 튜플로 저장

        seen.add(orig)

    return matched, unmatched



# ─── 6. MySQL 데이터베이스 연결 ────────────────────────────────
def create_mysql_connection():
    try:
        connection = mysql.connector.connect(
            host='152.69.235.49',
            port=3306,       # MySQL 서버 주소
            database='comhere',       # 데이터베이스 이름
            user='comhere88',    # MySQL 사용자명
            password='comHere88512!' # MySQL 비밀번호
        )
        if connection.is_connected():
            print("✅ MySQL 데이터베이스 연결 성공")
            return connection
    except Error as e:
        print(f"❌ MySQL 연결 실패: {e}")
        return None

# ─── 7. 매칭된 CPU 정보 저장 ────────────────────────────────────
# def save_cpu_matched_data(connection, matched_cpu_data):
#     if not matched_cpu_data:
#         print("💾 저장할 CPU 매칭 데이터가 없습니다.")
#         return 0

#     cursor = connection.cursor()

#     create_table_query = """
#     CREATE TABLE IF NOT EXISTS cpu_detailed_matches (
#         cpu_id INT AUTO_INCREMENT PRIMARY KEY,
#         model VARCHAR(255) NOT NULL,
#         cores INT,
#         threads INT,
#         base_clock_ghz FLOAT,
#         boost_clock_ghz FLOAT,
#         tdp_watt INT,
#         graphics VARCHAR(255),
#         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#     ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
#     """
#     try:
#         cursor.execute(create_table_query)
#         connection.commit()
#         print("✅ 테이블 생성/확인 완료")
#     except Error as e:
#         print(f"❌ 테이블 생성 실패: {e}")
#         cursor.close()
#         return 0

#     try:
#         cursor.execute("DELETE FROM cpu_detailed_matches")
#         connection.commit()
#         cursor.execute("ALTER TABLE cpu_detailed_matches AUTO_INCREMENT = 1")
#         connection.commit()
#         print("🗑️ 기존 데이터 삭제 및 AUTO_INCREMENT 초기화 완료")
#     except Error as e:
#         print(f"⚠️ 데이터 삭제 또는 AUTO_INCREMENT 초기화 실패: {e}")

#     insert_query = """
#     INSERT INTO cpu_detailed_matches (
#         model, cores, threads, base_clock_ghz, boost_clock_ghz, tdp_watt, graphics
#     ) VALUES (%s, %s, %s, %s, %s, %s, %s)
#     """

#     inserted_count = 0

#     for match in matched_cpu_data:
#         details = match["cpu_details"]
#         data_tuple = (
#             match["normalized_name"],
#             int(details.get("cores", 0)) if details.get("cores") else None,
#             int(details.get("threads", 0)) if details.get("threads") else None,
#             float(details.get("base_clock", 0)) if details.get("base_clock") else None,
#             float(details.get("boost_clock", 0)) if details.get("boost_clock") else None,
#             int(details.get("tdp", 0)) if details.get("tdp") else None,
#             details.get("graphics")
#         )

#         try:
#             cursor.execute(insert_query, data_tuple)
#             inserted_count += 1
#         except Error as e:
#             print(f"❌ 데이터 삽입 실패 ({match['normalized_name']}): {e}")

#     try:
#         connection.commit()
#         cursor.close()
#         print(f"✅ CPU 매칭 데이터 {inserted_count}개 저장 완료")
#         return inserted_count
#     except Error as e:
#         print(f"❌ 커밋 실패: {e}")
#         connection.rollback()
#         cursor.close()
#         return 0

# ─── 7. 매칭 + 매칭 안된 CPU 정보 저장 ────────────────────────────────────
def save_cpu_matched_data(connection, matched_cpu_data, unmatched_cpu_list):
    if not matched_cpu_data and not unmatched_cpu_list:
        print("💾 저장할 CPU 데이터가 없습니다.")
        return 0

    cursor = connection.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS cpu_detailed_matches (
        cpu_id INT AUTO_INCREMENT PRIMARY KEY,
        model VARCHAR(255) NOT NULL,
        cores INT,
        threads INT,
        base_clock_ghz FLOAT,
        boost_clock_ghz FLOAT,
        tdp_watt INT,
        graphics VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        cursor.execute(create_table_query)
        connection.commit()
        print("✅ 테이블 생성/확인 완료")
    except Error as e:
        print(f"❌ 테이블 생성 실패: {e}")
        cursor.close()
        return 0

    try:
        cursor.execute("DELETE FROM cpu_detailed_matches")
        connection.commit()
        cursor.execute("ALTER TABLE cpu_detailed_matches AUTO_INCREMENT = 1")
        connection.commit()
        print("🗑️ 기존 데이터 삭제 및 AUTO_INCREMENT 초기화 완료")
    except Error as e:
        print(f"⚠️ 데이터 삭제 또는 AUTO_INCREMENT 초기화 실패: {e}")

    insert_query = """
    INSERT INTO cpu_detailed_matches (
        model, cores, threads, base_clock_ghz, boost_clock_ghz, tdp_watt, graphics
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    inserted_count = 0

    for match in matched_cpu_data:
        details = match["cpu_details"]
        data_tuple = (
            match["normalized_name"],
            int(details.get("cores", 0)) if details.get("cores") else None,
            int(details.get("threads", 0)) if details.get("threads") else None,
            float(details.get("base_clock", 0)) if details.get("base_clock") else None,
            float(details.get("boost_clock", 0)) if details.get("boost_clock") else None,
            int(details.get("tdp", 0)) if details.get("tdp") else None,
            details.get("graphics")
        )
        try:
            cursor.execute(insert_query, data_tuple)
            inserted_count += 1
        except Error as e:
            print(f"❌ 데이터 삽입 실패 ({match['normalized_name']}): {e}")

    for original, normalized in unmatched_cpu_list:
        try:
            cursor.execute(insert_query, (normalized, None, None, None, None, None, None))
            inserted_count += 1
        except Error as e:
            print(f"❌ 매칭 실패 모델 삽입 실패 ({normalized}): {e}")

    try:
        connection.commit()
        cursor.close()
        print(f"✅ CPU 데이터 {inserted_count}개 저장 완료 (매칭 + 미매칭 포함)")
        return inserted_count
    except Error as e:
        print(f"❌ 커밋 실패: {e}")
        connection.rollback()
        cursor.close()
        return 0



# ─── 8. 메인 실행 함수 ─────────────────────────────────────────
def main_cpu(excel_path, json_path):
    df = pd.read_excel(excel_path, engine='openpyxl')
    df = df.drop(index=list(range(0, 4)) + list(range(129, len(df))))
    first_col = df.columns[0]

    print("📦 JSON 모델 로드 중...")
    json_models_dict = load_json_cpu_models(json_path)

    print("🌐 API CPU 데이터 로드 중...")
    api_models_dict = load_api_cpu_models()

    variants, excluded = create_cpu_variants(df, first_col)
    matched, unmatched = match_cpu_variants(variants, json_models_dict, api_models_dict)

    print("\n🎯 매칭 결과 요약")
    print("=" * 60)
    print(f"✅ 매칭 성공: {len(matched)}개")
    print(f"❌ 매칭 실패: {len(unmatched)}개")
    print(f"🚫 제외된 항목: {len(excluded)}개")

    if matched:
        print("\n📋 매칭된 CPU:")
        for m in matched:
            d = m["cpu_details"]
            print(f"- {m['original']} → {d['model']} ({d.get('source', 'json')}), {d.get('cores')}C/{d.get('threads')}T, {d.get('base_clock')}→{d.get('boost_clock')}GHz, {d.get('tdp')}W")

    connection = create_mysql_connection()
    if connection:
        save_cpu_matched_data(connection, matched, unmatched)
        connection.close()
        print("🔌 MySQL 연결 종료")


# ─── 9. 실행 경로 지정 ─────────────────────────────────────────
if __name__ == "__main__":
    excel_path = 'data/CPU 가성비 (25년 6월) v1.0.xlsx'
    json_path = 'cpu.json'
    main_cpu(excel_path, json_path)
