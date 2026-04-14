# DART 재무제표 수집 & 분석 파이프라인

한국 주식시장 기업의 재무제표를 DART OpenAPI로 수집하여 30개 재무비율 CSV를 생성하고, S3에 업로드 후 ML 분석까지 수행하는 end-to-end 파이프라인이다.

## 파이프라인 개요

```
DART API
  │
  ▼
┌─────────────────────────────────────────────────────┐
│  1. 수집 (Collection)                                │
│  DART API → raw JSON 저장                            │
│  data/raw/{status}/{sector}/{ticker}_{year}_{Q}.json │
└─────────────────┬───────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────┐
│  2. 전처리 (Preprocessing)              preprocess/  │
│                                                      │
│  account_mapper.py   계정과목명 → 표준 키 매핑        │
│  ratio_calculator.py 표준 키 → 재무비율 계산          │
│  etl.py              JSON → 기업별 CSV 변환 + CLI     │
│  step1_build_combined.py                             │
│    raw JSON 전체 → combined_raw.csv 생성             │
│  step2_label_and_preprocess.py                       │
│    rolling label + split + 결측/이상치 처리          │
│                                                      │
│  출력:                                               │
│    data/output/{sector}/{ticker}_{year}.csv          │
│    preprocess/data/processed/combined_raw.csv        │
│    preprocess/data/processed/H*/train|valid|test.csv │
└─────────────────┬───────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────┐
│  3. S3 업로드 & 조회                      s3/        │
│                                                      │
│  uploader.py   CSV/JSON → S3 업로드                  │
│  query.py      S3 오브젝트 필터링 조회               │
│  cli.py        CLI (by-status, by-sector, ...)       │
└─────────────────┬───────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────┐
│  4. 분석 (Analysis)                         eda/      │
│                                                      │
│  utils.py      데이터 로드, 결측 분석, 시각화,       │
│                상관관계, 이상치 탐지 유틸리티          │
│  eda_template.ipynb  EDA 실행 노트북                 │
└─────────────────────────────────────────────────────┘
```

## 재무비율 (30개)

| 카테고리 | 비율 |
|---------|------|
| 성장성 (5) | 총자산증가율, 유동자산증가율, 매출액증가율, 순이익증가율, 영업이익증가율 |
| 수익성 (3) | 매출액순이익률, 매출총이익률, 자기자본순이익률 |
| 활동성 (5) | 매출채권회전율, 재고자산회전율, 총자본회전율, 유형자산회전율, 매출원가율 |
| 안정성 (13) | 부채비율, 유동비율, 자기자본비율, 당좌비율, 비유동자산장기적합률, 순운전자본비율, 차입금의존도, 현금비율, 유형자산, 무형자산, 무형자산상각비, 유형자산상각비, 감가상각비 |
| 가치평가 (4) | 총자본영업이익률, 총자본순이익률, 유보액/납입자본비율, 총자본투자효율 |

## 프로젝트 구조

```
├── preprocess/              # 전처리 및 학습용 데이터셋 생성
│   ├── step1_build_combined.py
│   │                       #   raw JSON 전체 → combined_raw.csv
│   ├── step2_label_and_preprocess.py
│   │                       #   H별 라벨링 + split + 결측/이상치 처리
│   ├── preprocessor.py     #   clean_data 생성용 정제 파이프라인
│   └── src/
│       ├── account_mapper.py
│       ├── ratio_calculator.py
│       └── etl.py          #   raw JSON → CSV 변환 + CLI
│
├── s3/                      # S3 업로드 & 조회
│   ├── cli.py               #   CLI 진입점 (by-status, by-sector, ...)
│   ├── query.py             #   S3 오브젝트 필터링 조회
│   └── uploader.py          #   S3 업로드 (KST 타임존)
│
├── eda/                     # EDA 유틸리티 및 노트북
│   ├── utils.py             #   데이터 로드, 결측/이상치 분석, 시각화
│   ├── eda_template.ipynb   #   EDA 템플릿 노트북
│   └── eda_template.executed.html
│
├── scripts/                 # 자동화 스크립트
│   └── pr_pipeline.py       #   PR 분석 파이프라인
│
├── docs/                    # 문서
│   ├── data_contract_for_A.md
│   ├── raw_schema_check.md
│   └── b_work_plan.md
│
├── data/
│   ├── input/               # 수집 대상 기업 목록 CSV
│   ├── raw/                 # DART API 원본 JSON
│   └── output/              # 재무비율 CSV
│
└── requirements.txt
```

## 설치

```bash
pip install -r requirements.txt
```

필수 환경변수 (`.env`):
- `DART_API_KEY`
- `S3_ACCESS_KEY`, `S3_PRIVATE_KEY`, `S3_BUCKET_NAME`

## 사용법

### 재무비율 CSV 변환

```bash
# 단일 기업
python -m preprocess.src.etl single \
    --raw-dir data/raw/sample/healthy/Materials \
    --ticker 001810 --year 2025 --corp-name 무림SP --label 0 \
    --output data/output/sample/Materials/001810_2025.csv

# 일괄 변환
python -m preprocess.src.etl batch \
    --raw-base data/raw/sample \
    --output-base data/output/sample \
    --company-csv data/input/companies.csv
```

### 학습용 데이터셋 생성

```bash
# Step 1: raw JSON 전체를 재무비율 행으로 병합
python preprocess/step1_build_combined.py

# Step 2: rolling label 생성 후 H별 train/valid/test 저장
python preprocess/step2_label_and_preprocess.py
```

- Step 1 출력: `preprocess/data/processed/combined_raw.csv`
- Step 2 출력: `preprocess/data/processed/H6`, `H8`, ..., `H24`
- 각 H 디렉터리에는 `train.csv`, `valid.csv`, `test.csv`, `meta.json`이 저장된다.

### S3 데이터 조회

```bash
python -m s3.cli by-status          # 정상/상폐별 건수
python -m s3.cli by-sector          # GICS 섹터별 건수
python -m s3.cli by-year            # 연도별 건수
python -m s3.cli by-ticker          # 기업코드별 건수
python -m s3.cli sectors            # 섹터 목록
```

공통 필터: `--status healthy|delisted`, `--sector`, `--ticker`, `--year`, `--quarter`, `--json`

## 데이터 규칙

- S3 키: `{healthy|delisted}/{gics_sector}/{ticker}_{year}_{quarter}.json`
- 출력 CSV: `data/output/{sector}/{ticker}_{year}.csv`
- 기업 목록: `data/input/companies_*.csv` (stock_code, corp_name, gics_sector, start_year, end_year, label)
