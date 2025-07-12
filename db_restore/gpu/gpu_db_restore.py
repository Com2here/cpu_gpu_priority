import pandas as pd
import re
import mysql.connector
from mysql.connector import Error

# ─── GPU 이름 정규화 함수들 ───────────────────────────────
def normalize_model_name(name):
    if not name or not isinstance(name, str):
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

def delete_model_gddr(name):
    return re.sub(r'\s+gddr\d+x?', '', name.strip(), flags=re.IGNORECASE)

def extract_model_and_vram(name):
    match = re.search(r"(.*?)(?:\s+(\d+)\s*gb)?$", name.strip(), flags=re.IGNORECASE)
    if match:
        model = match.group(1).strip()
        vram = int(match.group(2)) if match.group(2) else None
        return model, vram
    return name, None

# ─── DB 연결 함수 ───────────────────────────────
def create_mysql_connection():
    try:
        connection = mysql.connector.connect(
            host='3.36.156.161',
            port=3306,
            database='comhere',
            user='comhere88',
            password='comHere88512!'
        )
        if connection.is_connected():
            print("✅ MySQL 연결 성공")
            return connection
    except Error as e:
        print(f"❌ MySQL 연결 실패: {e}")
    return None

# ─── 메인 로직 ───────────────────────────────
def update_gpu_priority_to_db(csv_path):
    # 1. CSV 읽기
    df = pd.read_csv(csv_path)

    # 2. 모델명 정규화
    df["모델명_정규화"] = df["GPU명"].apply(lambda x: extract_model_and_vram(
        delete_model_gddr(normalize_model_name(x))
    )[0])

    # 3. DB 연결
    conn = create_mysql_connection()
    if not conn:
        return
    cursor = conn.cursor()

    # 4. 업데이트 쿼리 (chipset 기준)
    update_query = """
    UPDATE gpu
    SET total_score = %s,
        pure_score = %s,
        price = %s
    WHERE chipset = %s
    """
    # ▼ 4. 업데이트 쿼리 (조건별로 동적 처리)
    update_count = 0

    for _, row in df.iterrows():
        try:
            norm_name = row["모델명_정규화"]
            if not isinstance(norm_name, str) or not norm_name.strip():
                continue

            total_score = row.get("종합_성능점수")
            pure_score = row.get("순수_성능점수")
            price = row.get("GPU_가격")

            update_fields = []
            update_values = []

            if pd.notna(total_score):
                update_fields.append("total_score = %s")
                update_values.append(float(total_score))

            if pd.notna(pure_score):
                update_fields.append("pure_score = %s")
                update_values.append(float(pure_score))

            if pd.notna(price):
                update_fields.append("price = %s")
                update_values.append(int(price))

            if not update_fields:
                continue  # 업데이트할 내용이 없음

            query = f"""
            UPDATE gpu
            SET {', '.join(update_fields)}
            WHERE chipset = %s
            """
            update_values.append(norm_name)
            cursor.execute(query, tuple(update_values))
            update_count += cursor.rowcount

        except Exception as e:
            print(f"❌ 업데이트 실패: {row['GPU명']} → {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ 업데이트 완료: {update_count}개 항목 적용됨")

# ─── 실행 ───────────────────────────────
update_gpu_priority_to_db("../../gpu/gpu_total_priority_price.csv")
