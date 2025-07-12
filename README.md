# cpu_gpu_priority

## 의존성 설치

```
pip install pandas numpy scikit-learn openpyxl
```

## DB 저장

1. cpu/cpu_csv_restore.py | gpu/gpu_csv_restore.py

- csv파일에서 각 부품에 대한 정보를 정규화하여 새로운 csv파일로 저장

2. db_restore/cpu/cpu_db_restore.py | db_restore/gpu/gpu_db_restore.py

- 1번에서 생성한 csv파일을 기반으로 다른 부품 정보와 결합하여 DB에 저장
