# S3 데이터 조회 CLI 사용법

## 실행 방법

```bash
python -m s3.cli <command> [options]
```

---

## 서브커맨드

### `by-status` — 정상/상폐별 건수 조회

```bash
# 전체 조회
python -m s3.cli by-status

# 특정 섹터만
python -m s3.cli by-status --sector "Information Technology"

# 2023년 데이터만
python -m s3.cli by-status --year 2023

# JSON 출력
python -m s3.cli by-status --json
```

### `by-sector` — 상태별 GICS 섹터별 건수 조회

```bash
# 전체 조회
python -m s3.cli by-sector

# 정상 기업만
python -m s3.cli by-sector --status healthy

# 상폐 기업만
python -m s3.cli by-sector --status delisted

# 특정 연도 + 분기
python -m s3.cli by-sector --year 2023 --quarter Q1
```

### `by-year` — 연도별 건수 조회

```bash
# 전체 조회
python -m s3.cli by-year

# 특정 기업코드
python -m s3.cli by-year --ticker 005930

# 상폐 기업의 연도별
python -m s3.cli by-year --status delisted
```

### `by-ticker` — 기업코드별 건수 조회

```bash
# 전체 조회
python -m s3.cli by-ticker

# 특정 섹터 내 기업별
python -m s3.cli by-ticker --sector "Energy"

# 정상 기업 중 2023년
python -m s3.cli by-ticker --status healthy --year 2023
```

### `sectors` — 섹터 목록 조회

```bash
# 전체 섹터 목록
python -m s3.cli sectors

# 정상 기업 섹터만
python -m s3.cli sectors --status healthy
```

---

## 공통 옵션

| 옵션 | 설명 | 예시 |
|---|---|---|
| `--status` | 정상(`healthy`) / 상폐(`delisted`) 필터 | `--status healthy` |
| `--sector` | GICS 섹터 필터 | `--sector "Industrials"` |
| `--ticker` | 6자리 종목코드 필터 | `--ticker 005930` |
| `--year` | 연도 필터 | `--year 2023` |
| `--quarter` | 분기 필터 | `--quarter Q1` |
| `--json` | JSON 형식으로 출력 | `--json` |
| `--bucket` | S3 버킷 이름 (`.env` 미설정 시) | `--bucket my-bucket` |
| `--region` | AWS 리전 | `--region ap-northeast-2` |

> `--json` 과 `--bucket`, `--region`은 `sectors` 커맨드에서도 사용 가능하나, `sectors`는 `--sector`, `--ticker`, `--year`, `--quarter` 필터를 지원하지 않습니다.

---

## 필터 조합 예시

```bash
# 정상 기업 중 Information Technology 섹터의 2023년 Q1 데이터 건수를 연도별로
python -m s3.cli by-year --status healthy --sector "Information Technology" --quarter Q1 --year 2023

# 상폐 기업 중 005930 기업의 데이터를 연도별로
python -m s3.cli by-year --status delisted --ticker 005930

# 전체 데이터를 섹터별로 보되 JSON으로 출력
python -m s3.cli by-sector --json
```
