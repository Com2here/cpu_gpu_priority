import pandas as pd
import re
import mysql.connector
from mysql.connector import Error

# ─── GPU 이름 정규화 함수들 ───────────────────────────────
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
            host='152.69.235.49',
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
# ─── 메인 로직 ───────────────────────────────
def update_gpu_line_priority_to_db(csv_path):
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

    # 4. DB 업데이트 쿼리 (라인 포함)
    update_query = """
    UPDATE gpu_detailed_matches
    SET line = %s,
        line_total_score_rank = %s,
        line_pure_score_rank = %s
    WHERE chipset = %s
    """

    update_count = 0
    for _, row in df.iterrows():
        try:
            normalized_chipset = row["모델명_정규화"]
            gpu_line = row["라인"] if pd.notna(row["라인"]) else None
            rank_total = int(row["라인_내_종합_성능_순위"]) if pd.notna(row["라인_내_종합_성능_순위"]) else None
            rank_pure = int(row["라인_내_순수_성능_순위"]) if pd.notna(row["라인_내_순수_성능_순위"]) else None

            cursor.execute(update_query, (
                gpu_line,
                rank_total,
                rank_pure,
                normalized_chipset
            ))
            update_count += cursor.rowcount
        except Exception as e:
            print(f"❌ 업데이트 실패: {row['GPU명']} → {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ 업데이트 완료: {update_count}개 항목 적용됨")

# ─── 실행 ───────────────────────────────
update_gpu_line_priority_to_db("./gpu/gpu_line_priority.csv")
