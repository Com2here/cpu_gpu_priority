import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np

# ▼ 1. 엑셀 업로드 및 읽기
df = pd.read_excel("..\data\그래픽카드 가성비 (25년 6월) v1.1.xlsx", header=2)


# ▼ 2. 컬럼명 정리
df = df.rename(columns={
    df.columns[0]: "GPU명",
    df.columns[1]: "게임성능_FHD",
    df.columns[2]: "게임성능_QHD",
    df.columns[3]: "게임성능_UHD",
    df.columns[4]: "파스점수",
    df.columns[5]: "타스점수",
    df.columns[6]: "스노점수",
    df.columns[7]: "블렌더점수",
    df.columns[8]: "FPS_FHD",
    df.columns[9]: "FPS_QHD",
    df.columns[10]: "FPS_UHD",
    # df.columns[12]: "GPU_가격",
    df.columns[14]: "가성비_FHD",
})

# ▼ 0. 파일 경로 및 컬럼명 정의
price_column = df.columns[12]  # 이미지 기준 "당월(3종 평균)"에 해당하는 열

# ▼ A. 가격 정보 추출
df["GPU_가격"] = (
    df[price_column]
    .astype(str)
    .str.replace(",", "")
    .str.replace("원", "")
    .str.extract(r'(\d+)')  # 숫자만 추출
    .astype(float)
)

# ▼ 2.5 GPU 라인 분류: 라인명 탐지 후 역방향으로 전파
line_keywords = ["하이엔드", "퍼포먼스", "상위 메인스트림", "하위 메인스트림", "엔트리", "로우엔드"]
current_line = None
lines = [None] * len(df)

for i in reversed(range(len(df))):
    cell_text = str(df.loc[i, "GPU명"])
    found = False
    for keyword in line_keywords:
        if keyword in cell_text and "라인" in cell_text:
            current_line = keyword
            found = True
            break
    if not found and pd.notna(df.loc[i, "게임성능_FHD"]):
        lines[i] = current_line

df["라인"] = lines  # df에 라인 컬럼 추가
df = df[df["라인"].notna()].copy()

# ▼ 3. 사용할 컬럼 선택 (라인 포함)
target_columns = [
    "GPU명", "라인",
    "게임성능_FHD", "게임성능_QHD", "게임성능_UHD",
    "파스점수", "타스점수", "스노점수",
    "블렌더점수",
    "FPS_FHD", "FPS_QHD", "FPS_UHD",
    "가성비_FHD",
    "GPU_가격"
]
df = df[target_columns]

# ▼ 4. 숫자형 변환
for col in target_columns[2:]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# ▼ 5. 가중치 설정
weights_total = {
    "게임성능_FHD": 0.2, "게임성능_QHD": 0.2, "게임성능_UHD": 0.2,
    "파스점수": 0.05, "타스점수": 0.05, "스노점수": 0.05,
    "블렌더점수": 0.07,
    "FPS_FHD": 0.01, "FPS_QHD": 0.01, "FPS_UHD": 0.01,
    "가성비_FHD": -0.075
}
weights_pure = weights_total.copy()
weights_pure.pop("가성비_FHD")

# ▼ 6. 정규화
df_norm = df.copy()
scaler = MinMaxScaler()
norm_cols = list(weights_total.keys())
df_norm[[f"{col}_norm" for col in norm_cols]] = scaler.fit_transform(df[norm_cols])

# ▼ 7. 점수 계산 함수 정의
def compute_score(row, weights):
    valid_weights = {k: w for k, w in weights.items() if not pd.isna(row[f"{k}_norm"])}
    weight_sum = sum(abs(w) for w in valid_weights.values())
    if weight_sum < 0.5:
        return np.nan, weight_sum
    scale = 1 / weight_sum if weight_sum < 1.0 else 1.0
    score = sum(row[f"{k}_norm"] * w * scale for k, w in valid_weights.items())
    return score, weight_sum

# ▼ 8. 점수 및 유효 가중치 계산
df_norm["종합_성능점수"], df_norm["유효가중치_종합"] = zip(*df_norm.apply(lambda row: compute_score(row, weights_total), axis=1))
df_norm["순수_성능점수"], df_norm["유효가중치_순수"] = zip(*df_norm.apply(lambda row: compute_score(row, weights_pure), axis=1))

# ▼ 9. 전체 순위 계산
df_norm["종합_성능_순위"] = df_norm["종합_성능점수"].rank(ascending=False, method='min', na_option='bottom').fillna(999).astype(int)
df_norm["순수_성능_순위"] = df_norm["순수_성능점수"].rank(ascending=False, method='min', na_option='bottom').fillna(999).astype(int)

# ▼ 10. 라인 내 순위 계산
df_norm["라인_내_종합_성능_순위"] = (
    df_norm.groupby("라인")["종합_성능점수"]
    .rank(ascending=False, method='min')
    .fillna(999)
    .astype(int)
)
df_norm["라인_내_순수_성능_순위"] = (
    df_norm.groupby("라인")["순수_성능점수"]
    .rank(ascending=False, method='min')
    .fillna(999)
    .astype(int)
)

# ▼ 11. 전체 순위 출력
df_total_result = df_norm.sort_values(by="종합_성능_순위").reset_index(drop=True)[[
    "GPU명", "라인", "GPU_가격",
    "종합_성능점수", "유효가중치_종합", "종합_성능_순위",
    "순수_성능점수", "유효가중치_순수", "순수_성능_순위"
]]

# ▼ 12. 라인별 내부 순위 출력
df_line_result = df_norm.sort_values(by=["라인", "라인_내_종합_성능_순위"]).reset_index(drop=True)[[
    "GPU명", "라인", "유효가중치_종합",
    "종합_성능점수", "라인_내_종합_성능_순위", "유효가중치_순수",
    "순수_성능점수", "라인_내_순수_성능_순위"
]]

# ▼ 13. CSV 저장
df_total_result.to_csv("gpu_total_priority_price.csv", index=False, encoding="utf-8-sig")
# df_line_result.to_csv("gpu_line_priority.csv", index=False, encoding="utf-8-sig")

# 유효 가중치 조건을 만족하는 GPU만 필터링
df_line_result = df_line_result[
    (df_line_result["유효가중치_종합"] >= 0.5) & (df_line_result["유효가중치_순수"] >= 0.5)
].copy()

print("✅ 전체 GPU 종합/순수 성능 순위")
print(df_total_result.head(200))