import pandas as pd
import mysql.connector
from mysql.connector import Error
import re

# ─── 1. 정규화 함수 ───────────────────────────────
def normalize_cpu_model(name: str) -> str:
    if not name:
        return ""

    name = name.strip()
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

    name = re.sub(r'\b(A\d{2})(\d{4})([A-Z]*)\b', r'\1-\2\3', name, flags=re.IGNORECASE)
    name = re.sub(r'\b(FX)(\d{4})([A-Z]*)\b', r'\1-\2\3', name, flags=re.IGNORECASE)
    name = re.sub(r'\b(i[3579])(\d{4})([A-Z]*)\b', r'\1-\2\3', name, flags=re.IGNORECASE)
    name = re.sub(r'[^\w\s\-]', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

# ─── 2. CSV 파일 읽기 ─────────────────────────────
csv_path = "./cpu/CPU_라인별_성능_순위.csv"
df = pd.read_csv(csv_path)

# ─── 3. 정규화 컬럼 생성 ─────────────────────────
df["정규화명"] = df["CPU명"].apply(normalize_cpu_model)

# ─── 4. DB 연결 ────────────────────────────────
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

# ─── 5. 라인별 성능 순위 업데이트 ────────────────
def update_line_rankings(df):
    conn = create_mysql_connection()
    if not conn:
        return

    cursor = conn.cursor()
    updated = 0

    for _, row in df.iterrows():
        norm_name = row["정규화명"]
        line_total = row["라인_내_종합_성능_순위"]
        line_pure = row["라인_내_순수_성능_순위"]
        line_name = row["라인"]

        if not norm_name or pd.isna(line_total) or pd.isna(line_pure) or pd.isna(line_name):
            continue

        try:
            query = """
                UPDATE cpu_detailed_matches
                SET line_total_score_rank = %s,
                    line_pure_score_rank = %s,
                    line = %s
                WHERE model = %s
            """
            cursor.execute(query, (int(line_total), int(line_pure), line_name, norm_name))
            if cursor.rowcount > 0:
                updated += 1
        except Error as e:
            print(f"❌ 업데이트 실패: {norm_name}, {e}")

    conn.commit()
    print(f"✅ 라인별 순위 업데이트 완료: {updated}개")
    cursor.close()
    conn.close()

# ─── 6. 실행 ─────────────────────────────────────
update_line_rankings(df)
