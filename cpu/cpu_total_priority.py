import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import re

# ▼ 2. 엑셀 읽기
df = pd.read_excel("..\data\CPU 가성비 (25년 6월) v1.0.xlsx", header=3)

# ▼ 3. 컬럼명 정리
df = df.rename(columns={
    df.columns[0]: "CPU명",
    df.columns[1]: "게임성능_4090",
    df.columns[2]: "게임성능_5070",
    df.columns[3]: "게임성능_4060Ti",
    df.columns[4]: "게임성능_3050",
    df.columns[5]: "시네벤치_싱글",
    df.columns[6]: "시네벤치_멀티",
    df.columns[8]: "CPU_가격",
    df.columns[13]: "게이밍_가성비"
})

# ▼ 4. 라벨 역방향 채우기: 아래쪽에서 라벨 선언 → 위쪽에 적용
level_keywords = ["하이엔드", "퍼포먼스", "메인스트림", "엔트리"]
current_level = None
levels = [None] * len(df)

for i in reversed(range(len(df))):  # 역순으로 순회
    cell_text = str(df.loc[i, "CPU명"])
    found = False
    for keyword in level_keywords:
        if keyword in cell_text and "라인" in cell_text:
            current_level = keyword
            found = True
            break
    if not found and pd.notna(df.loc[i, "게이밍_가성비"]):
        levels[i] = current_level

df["라인"] = levels

# ▼ 5. CPU 외의 행 제거
df["게이밍_가성비"] = pd.to_numeric(df["게이밍_가성비"].astype(str).str.replace(r"[^\d\.]", "", regex=True), errors="coerce")

# ▼ 6. 라인별 GPU 성능 선택
def select_game_score(row):
    if row['라인'] == '하이엔드':
        return row['게임성능_4090']
    elif row['라인'] == '퍼포먼스':
        return row['게임성능_5070']
    elif row['라인'] == '메인스트림':
        return row['게임성능_4060Ti']
    elif row['라인'] == '엔트리':
        return row['게임성능_3050']
    return None

df["선택_게임성능"] = df.apply(select_game_score, axis=1)

# ▼ 7. 숫자형 변환
for col in ["선택_게임성능", "시네벤치_멀티", "시네벤치_싱글", "게이밍_가성비", "CPU_가격"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# ▼ 8. 정규화
scaler = MinMaxScaler()
norm_cols = ["선택_게임성능", "시네벤치_멀티", "시네벤치_싱글", "게이밍_가성비", "CPU_가격"]
normalized = pd.DataFrame(scaler.fit_transform(df[norm_cols]), columns=[f"{col}_norm" for col in norm_cols])
df = pd.concat([df, normalized], axis=1)

# ▼ 9. 점수 계산
df["종합_성능점수"] = (
    df["선택_게임성능_norm"] * 0.5 +
    df["시네벤치_멀티_norm"] * 0.2 +
    df["시네벤치_싱글_norm"] * 0.1 +
    (1 - df["게이밍_가성비_norm"]) * 0.05 -
    df["CPU_가격_norm"] * 0.1
)

df["순수_성능점수"] = (
    df["선택_게임성능_norm"] * 0.6 +
    df["시네벤치_멀티_norm"] * 0.3 +
    df["시네벤치_싱글_norm"] * 0.1
)

# ▼ 10. 순위 계산
df["종합_성능_순위"] = df["종합_성능점수"].rank(ascending=False, method='min')
df["순수_성능_순위"] = df["순수_성능점수"].rank(ascending=False, method='min')
df["종합_성능_순위"] = df["종합_성능_순위"].fillna(999).astype(int)
df["순수_성능_순위"] = df["순수_성능_순위"].fillna(999).astype(int)


df["라인_내_종합_성능_순위"] = (
    df.groupby("라인")["종합_성능점수"]
    .rank(ascending=False, method='min')
    .fillna(999)
    .astype(int)
)

df["라인_내_순수_성능_순위"] = (
    df.groupby("라인")["순수_성능점수"]
    .rank(ascending=False, method='min')
    .fillna(999)
    .astype(int)
)

# ▼ 13. 결과 출력
# 전체 성능 순위 기준 정렬
df_total_sorted = df.sort_values(by="종합_성능점수", ascending=False)

# 출력할 열 선택
columns_total = [
    "CPU명", "라인",
    "종합_성능점수", "종합_성능_순위",
    "순수_성능점수", "순수_성능_순위"
]

# 전체 순위 테이블 출력
print("전체 종합 성능 순위")
print(df_total_sorted[columns_total].reset_index(drop=True))

df_total_sorted[columns_total].reset_index(drop=True).to_csv("CPU_성능_순위_종합.csv", index=False, encoding="utf-8-sig")

