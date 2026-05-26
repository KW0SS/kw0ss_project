# 전처리 파이프라인 정리

생성일: 2026-05-26

---

## 1. 원본 Raw 데이터 현황

| 구분 | 숫자 코드 기업 | 알파 코드 기업 | 합계 |
|---|---|---|---|
| raw/healthy | 1,781 | 32 | 1,813 |
| raw/delisted | 170 | 0 | 170 |

알파 코드(0001A0 형태)는 DART에서 신규 부여한 코드 체계로, 주로 SPAC·리츠·특수목적법인입니다.

---

## 2. Step 1 — `build_master_dataset.py`

**입력:** `raw/healthy/`, `raw/delisted/` 전체 JSON  
**출력:** `data/processed/combined_raw.csv`

### 2-1. 파일명 패턴 필터링

정규식 `^\d{6}_\d{4}_(Q1|Q3|H1|ANNUAL)(_CFS|_OFS)?\.json$` 에 불일치하는 파일 제외.

**탈락: healthy 알파 코드 32개 기업**

| 코드 | 예시 기업명 |
|---|---|
| 0001A0 | 덕양에너젠 |
| 0004Y0 | 디비금융제14호스팩 |
| 0037T0 | KB제32호스팩 |
| 0068Y0 | 비엔케이제3호스팩 |
| (이하 28개 동일 패턴) | 주로 SPAC, 신규 상장 특수법인 |

### 2-2. EXCLUDE_CODES — 구조적 상폐 제거

경영 실패가 아닌 이유로 상폐된 기업 4개를 수동 제외. 이들을 포함하면 "상폐 = 경영 위기" 라벨 의미가 오염됨.

**탈락: delisted 4개 기업**

| 종목코드 | 기업명 | 상폐 사유 |
|---|---|---|
| 048260 | 오스템임플란트 | 자진상폐 |
| 029960 | 코엔텍 | 완전자회사 편입 |
| 006580 | 대양제지 | 자진상폐 |
| 115960 | 연우 | 완전자회사 편입 |

### 2-3. JSON → 재무비율 계산

`extract_standard_items()` + `compute_all_ratios()`로 30개 재무비율 계산. ProcessPoolExecutor 병렬 처리. JSON이 비어 있거나 파싱 오류면 해당 파일 제외.

### 2-4. YoY 증가율 계산

매출액증가율·순이익증가율·영업이익증가율을 **전기(frmtrm) 방식이 아닌 전년 동기(YoY)** 방식으로 계산.  
이유: DART IS 데이터에서 frmtrm 결측 빈번.

### 2-5. KRX 상장일 기반 비상장 기간 데이터 제거

**탈락: 83행 (10개 기업)**

종목코드 재사용 케이스 — 구 회사 상폐 후 새 회사가 같은 코드를 부여받는 경우, 신 회사 raw에 구 회사 데이터가 섞이는 문제 해결. `상장법인목록.xlsx`의 상장일 이전 분기 행 제거.

대표 사례: `036220` 오상헬스케어

---

## 3. combined_raw.csv 최종 결과

**파일 위치: `data/processed/combined_raw.csv`** ← 파이프라인 정식 경로

| 구분 | 기업 수 | 행 수 |
|---|---|---|
| healthy (label=0) | 1,779 | ~56,000 |
| delisted (label=1) | 166 | ~2,600 |
| **합계** | **1,945** | **~58,600** |

연도 범위: 2000~2025 / 컬럼: 35개 (식별자 5 + 재무비율 30)



---

## 4. Step 2 — `build_h_datasets.py` / `build_fixed_datasets.py`

**입력:** `data/processed/combined_raw.csv`  
**출력:** `data/processed_v*/`, `data/processed_fixed_v*/`

### 공통 전처리 순서

1. **2015년 이전 제거** — 데이터 희소성
2. **라벨 재정의**
   - H 방식(`build_h_datasets.py`): T 시점 기준 이후 H개월 내 상폐 여부 → rolling 라벨
   - N 방식(`build_fixed_datasets.py`): 상폐연도 기준 정확히 N년 전 연도 행만 label=1
3. **Time split:** train(2015~2022) / valid(2023) / test(2024)
4. **결측치 처리:** ffill → CF=0 보정 → 섹터·분기 중앙값 보간
5. **이상치 클리핑** (DOMAIN_RULES 기반)
6. **실험 설정 4종 저장**

| 실험 | Winsorize | RobustScaler |
|---|---|---|
| baseline | ✗ | ✗ |
| exp-A | ✓ | ✗ |
| exp-B | ✗ | ✓ |
| exp-C | ✓ | ✓ |

---