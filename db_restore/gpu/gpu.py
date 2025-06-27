import re
import pandas as pd
import json
import mysql.connector
from mysql.connector import Error
from pcpartpicker import API
from datetime import datetime

# ─── 1. 모델명 정규화 ─────────────────────────────────────────────
def normalize_model_name(name):
    if not name:
        return ""
    name = name.lower().strip()
    replacements = {
        "지포스": "geforce",
        "라데온": "radeon",
        "아크": "arc",
        "그래픽스": "graphics"
    }
    for kr, en in replacements.items():
        name = name.replace(kr, en)
    name = re.sub(r'[^\w\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

# ─── 2. GDDR 제거 ─────────────────────────────────────────────
def delete_model_gddr(name):
    return re.sub(r'\s+gddr\d+x?', '', name.strip(), flags=re.IGNORECASE)

# ─── 3. 칩셋명 + VRAM 분리 ─────────────────────────────────────
def extract_model_and_vram(name):
    match = re.search(r"(.*?)(?:\s+(\d+)\s*gb)?$", name.strip(), flags=re.IGNORECASE)
    if match:
        model = match.group(1).strip()
        vram = int(match.group(2)) if match.group(2) else None
        return model, vram
    return name, None

# ─── 4. 워크스테이션 GPU 필터 ──────────────────────────────────
def is_excludable_model(model):
    model = model.lower()
    return any(keyword in model for keyword in ["firepro", "quadro", "tesla", "rtx a", "radeon pro"])

# ─── 5. JSON 모델 로드 (상세 정보 포함) ─────────────────────────
def load_json_models_detailed(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    models_dict = {}  # normalized_name -> detailed_info
    
    for item in data:
        chipset = item.get("chipset", "").strip()
        if chipset and not is_excludable_model(chipset):
            normalized = normalize_model_name(chipset)
            
            # JSON에서 상세 정보 추출 (name, price, color 제외)
            detailed_info = {
                'original_chipset': chipset,
                'normalized_chipset': normalized,
                'memory': item.get('memory', None),
                'core_clock': item.get('core_clock', None),
                'boost_clock': item.get('boost_clock', None),
                'length': item.get('length', None)
            }
            
            models_dict[normalized] = detailed_info
    
    return models_dict

# ─── 6. API 모델 로드 ──────────────────────────────────────────
def load_api_models(region="us"):
    try:
        api = API(region)
        video_card_data = api.retrieve("video-card")
        video_card_dict = json.loads(video_card_data.to_json())
        raw_list = video_card_dict.get("video-card", [])

        print(f"📊 API에서 수신한 GPU 수: {len(raw_list)}")

        models = set()

        for item in raw_list:
            chipset = item.get("chipset", "").strip()
            if not chipset or is_excludable_model(chipset):
                continue
            model_only, _ = extract_model_and_vram(chipset)
            normalized = normalize_model_name(model_only)
            models.add(normalized)

        print(f"📊 정규화된 API 모델 수: {len(models)}")
        return models

    except Exception as e:
        print(f"❌ API 호출 실패: {e}")
        return set()

# ─── 7. 엑셀 → 정규화 후보군 추출 ──────────────────────────────
def create_variants(df, first_column):
    variants = []
    excluded = []

    for value in df[first_column]:
        if pd.notna(value):
            original = str(value).strip()
            if original == "게임 그래픽 옵션" or "라인" in original:
                excluded.append(original)
                continue

            gddr_removed = delete_model_gddr(original)
            model_only, _ = extract_model_and_vram(gddr_removed)

            variants.append({
                "original": original,
                "norm_gddr": normalize_model_name(original),
                "norm_gddr_removed": normalize_model_name(gddr_removed),
                "norm_model_only": normalize_model_name(model_only)
            })

    return variants, excluded

# ─── 8. 조건별 매칭 수행 (상세 정보 포함) ────────────────────────
def match_variants_detailed(variants, json_models_dict, api_models):
    matched_json = []
    matched_api = []
    unmatched = []

    seen_originals = set()

    for v in variants:
        orig = v["original"]
        gddr = v["norm_gddr"]
        no_gddr = v["norm_gddr_removed"]
        model_only = v["norm_model_only"]

        if orig in seen_originals:
            continue

        # 1. JSON 매칭 (GDDR 포함)
        if gddr in json_models_dict:
            matched_json.append({
                'excel_name': orig,
                'normalized_name': gddr,
                'match_type': 'JSON_GDDR_INTACT',
                'gpu_details': json_models_dict[gddr]
            })
            seen_originals.add(orig)
        # 2. JSON 매칭 (GDDR 제거)
        elif no_gddr in json_models_dict:
            matched_json.append({
                'excel_name': orig,
                'normalized_name': no_gddr,
                'match_type': 'JSON_GDDR_REMOVED',
                'gpu_details': json_models_dict[no_gddr]
            })
            seen_originals.add(orig)
        # 3. API 매칭 (VRAM 분리)
        elif model_only in api_models:
            matched_api.append({
                'excel_name': orig,
                'normalized_name': model_only,
                'match_type': 'API_VRAM_SEPARATED'
            })
            seen_originals.add(orig)
        else:
            unmatched.append(orig)

    return matched_json, matched_api, unmatched

# ─── 9. MySQL 데이터베이스 연결 ────────────────────────────────
def create_mysql_connection():
    """MySQL 데이터베이스 연결 생성"""
    try:
        connection = mysql.connector.connect(
            host='',        # MySQL 서버 주소
            database='',       # 데이터베이스 이름
            user='',    # MySQL 사용자명
            password='' # MySQL 비밀번호
        )
        if connection.is_connected():
            print("✅ MySQL 데이터베이스 연결 성공")
            return connection
    except Error as e:
        print(f"❌ MySQL 연결 실패: {e}")
        return None

# ─── 10. JSON 매칭 데이터 저장 ──────────────────────────────────
def save_json_matched_data(connection, matched_json_data):
    """JSON 매칭 중 GDDR 제거되지 않은 항목만 저장"""
    if not matched_json_data:
        print("💾 저장할 JSON 매칭 데이터가 없습니다.")
        return 0

    cursor = connection.cursor()

    # 테이블 생성
    create_table_query = """
    CREATE TABLE IF NOT EXISTS gpu_detailed_matches (
        video_card_id INT AUTO_INCREMENT PRIMARY KEY,
        chipset VARCHAR(255) NOT NULL,
        memory_gb INT,
        core_clock_mhz INT,
        boost_clock_mhz INT,
        length_mm INT,
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

    # 기존 데이터 삭제
    try:
        cursor.execute("DELETE FROM gpu_detailed_matches")
        connection.commit()
        print("🗑️ 기존 데이터 삭제 완료")

        # AUTO_INCREMENT 초기화
        cursor.execute("ALTER TABLE gpu_detailed_matches AUTO_INCREMENT = 1")
        connection.commit()
        print("🔄 AUTO_INCREMENT 값 초기화 완료")
    except Error as e:
        print(f"⚠️ 기존 데이터 삭제 또는 AUTO_INCREMENT 초기화 실패: {e}")


    # 데이터 삽입
    insert_query = """
    INSERT INTO gpu_detailed_matches (
        chipset, memory_gb, core_clock_mhz, boost_clock_mhz, length_mm
    ) VALUES (%s, %s, %s, %s, %s)
    """

    inserted_count = 0

    for match_data in matched_json_data:
        # ✅ GDDR 제거된 항목은 저장하지 않음
        if match_data.get('match_type') == 'JSON_GDDR_REMOVED':
            continue

        gpu_details = match_data['gpu_details']
        data_tuple = (
            match_data['normalized_name'],
            int(gpu_details.get('memory', 0)) if gpu_details.get('memory') else None,
            int(gpu_details.get('core_clock', 0)) if gpu_details.get('core_clock') else None,
            int(gpu_details.get('boost_clock', 0)) if gpu_details.get('boost_clock') else None,
            int(gpu_details.get('length', 0)) if gpu_details.get('length') else None
        )

        try:
            cursor.execute(insert_query, data_tuple)
            inserted_count += 1
        except Error as e:
            print(f"❌ 데이터 삽입 실패 ({match_data['normalized_name']}): {e}")

    try:
        connection.commit()
        cursor.close()
        print(f"✅ JSON 매칭 데이터 {inserted_count}개 저장 완료")
        return inserted_count
    except Error as e:
        print(f"❌ 데이터 커밋 실패: {e}")
        connection.rollback()
        cursor.close()
        return 0



# ─── 11. API 매칭 데이터 저장 ───────────────────────────────────
def save_api_matched_data(connection, matched_api_data):
    """API와 매칭된 데이터를 간단히 저장"""
    if not matched_api_data:
        print("💾 저장할 API 매칭 데이터가 없습니다.")
        return 0
    
    cursor = connection.cursor()
    
    # 간단한 API 매칭 테이블
    create_table_query = """
    CREATE TABLE IF NOT EXISTS gpu_api_matches (
        id INT AUTO_INCREMENT PRIMARY KEY,
        excel_name VARCHAR(255) NOT NULL,
        normalized_name VARCHAR(255) NOT NULL,
        match_type VARCHAR(50) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_excel_name (excel_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    
    try:
        cursor.execute(create_table_query)
        connection.commit()
    except Error as e:
        print(f"❌ API 매칭 테이블 생성 실패: {e}")
        cursor.close()
        return 0
    
    # 기존 데이터 삭제
    try:
        cursor.execute("DELETE FROM gpu_api_matches")
        connection.commit()
    except Error as e:
        print(f"⚠️ 기존 API 매칭 데이터 삭제 실패: {e}")
    
    # 데이터 삽입
    insert_query = "INSERT INTO gpu_api_matches (excel_name, normalized_name, match_type) VALUES (%s, %s, %s)"
    batch_data = [(data['excel_name'], data['normalized_name'], data['match_type']) for data in matched_api_data]
    
    try:
        cursor.executemany(insert_query, batch_data)
        connection.commit()
        cursor.close()
        print(f"✅ API 매칭 데이터 {len(matched_api_data)}개 저장 완료")
        return len(matched_api_data)
    except Error as e:
        print(f"❌ API 매칭 데이터 저장 실패: {e}")
        connection.rollback()
        cursor.close()
        return 0

# ─── 12. 메인 실행 함수 ─────────────────────────────────────────
def main(excel_path, json_path):
    # 데이터 로드
    df = pd.read_excel(excel_path, engine='openpyxl').drop([0, 1])
    first_col = df.columns[0]

    print("📦 JSON 모델 상세 정보 로드 중...")
    json_models_dict = load_json_models_detailed(json_path)
    api_models = load_api_models()

    print(f"📦 JSON 모델 수: {len(json_models_dict)}")
    print(f"🌐 API 모델 수: {len(api_models)}")

    # 매칭 수행
    variants, excluded = create_variants(df, first_col)
    matched_json, matched_api, unmatched = match_variants_detailed(variants, json_models_dict, api_models)

    # 콘솔 출력
    print("\n🎯 매칭 결과 요약")
    print("=" * 60)
    print(f"✅ JSON 매칭 성공: {len(matched_json)}개")
    print(f"🔹 API 매칭 성공 (저장 제외): {len(matched_api)}개")
    print(f"❌ 매칭 실패: {len(unmatched)}개")
    print(f"🚫 제외된 항목: {len(excluded)}개")

    # JSON 매칭 결과 상세 출력
    if matched_json:
        print(f"\n📋 JSON 매칭된 모델들:")
        for match in matched_json:
            details = match['gpu_details']
            print(f"  - {match['excel_name']} → {details['original_chipset']} ({match['match_type']})")
            print(f"    VRAM: {details.get('memory', 'N/A')}GB, 코어클럭: {details.get('core_clock', 'N/A')}MHz, 부스트클럭: {details.get('boost_clock', 'N/A')}MHz")

    # MySQL 데이터베이스에 저장
    print("\n" + "=" * 60)
    print("🗄️ MySQL 데이터베이스 저장 시작 (JSON 매칭만)")
    print("=" * 60)
    
    connection = create_mysql_connection()
    if connection:
        try:
            # JSON 매칭 데이터만 저장
            json_saved = save_json_matched_data(connection, matched_json)
            
            print(f"\n📊 데이터베이스 저장 완료")
            print(f"  - JSON 매칭 (상세): {json_saved}개")
            print(f"  - API 매칭: 저장 생략됨")
            print(f"  - 매칭 실패: {len(unmatched)}개")
            
        except Exception as e:
            print(f"❌ 데이터베이스 저장 중 오류 발생: {e}")
        finally:
            if connection.is_connected():
                connection.close()
                print("🔌 MySQL 연결 종료")
    else:
        print("❌ 데이터베이스 연결 실패로 저장을 건너뜁니다.")


# ─── 13. 실행 경로 지정 ────────────────────────────────────────
if __name__ == "__main__":
    excel_path = "./data/그래픽카드 가성비 (25년 5월) v1.1.xlsx"
    json_path = "video-card.json"
    main(excel_path, json_path)