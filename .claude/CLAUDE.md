# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 프로젝트

DART 재무제표 수집 파이프라인. 한국 주식시장 기업의 재무제표를 DART API로 수집하여 `data/output/`에 재무비율 CSV, `data/raw/`에 원본 JSON을 저장하고 S3에 업로드한다.

## 개발 환경

```bash
pip install -r requirements.txt
```

필수 환경변수 (`.env` 파일):
- `DART_API_KEY`, `S3_ACCESS_KEY`, `S3_PRIVATE_KEY`, `S3_BUCKET_NAME`, `S3_REGION` (기본: ap-northeast-2)

## 주요 명령어

```bash
# S3 데이터 조회
python -m s3.cli by-status          # 정상/상폐별 건수
python -m s3.cli by-sector          # GICS 섹터별 건수
python -m s3.cli by-year            # 연도별 건수
python -m s3.cli by-ticker          # 기업코드별 건수
python -m s3.cli sectors            # 섹터 목록

# PR 분석 파이프라인
python3 scripts/pr_pipeline.py --output-json prs/context.json
python3 scripts/pr_pipeline.py --dry-run     # 점검만 실행
```

S3 CLI 공통 필터: `--status healthy|delisted`, `--sector`, `--ticker`, `--year`, `--quarter`, `--json`

## 아키텍처

### S3 모듈 (`s3/`)
- `cli.py`: argparse 기반 CLI 진입점. 서브커맨드별 쿼리 실행
- `query.py`: S3 오브젝트 목록 조회 및 필터링 (상태, 섹터, 종목, 연도, 분기)
- `uploader.py`: S3 업로드 (KST 타임존 처리)

S3 키 규칙: `{healthy|delisted}/{gics_sector}/{ticker}_{year}_{quarter}.json`

### PR 파이프라인 (`scripts/pr_pipeline.py`)
3단계 파이프라인: git diff 파싱 → PR 타입 분류(data/structure/both) → 검증 및 마크다운 생성

핵심 데이터 구조:
- `DiffEntry`: git diff 변경 항목 (status, path, old_path)
- `CheckItem`: 검증 결과 (name, status=PASS/WARN/FAIL, summary, details)

### 데이터 구조
- `data/input/companies_*.csv`: 수집 대상 기업 목록 (stock_code, corp_name, gics_sector, start_year, end_year, label)
- `data/output/{종목코드}_{연도}.csv`: 재무비율 CSV (파일명 패턴: `XXXXXX_YYYY.csv`)
- `data/raw/`: DART API 원본 JSON

## PR 분석 워크플로우

사용자가 "PR 분석해줘", "PR 요약해줘" 등을 요청하면 아래 순서로 진행한다.

### 1단계: 파이프라인 실행
```bash
python3 scripts/pr_pipeline.py --output-json prs/context.json
```
- `prs/` 디렉터리에 PR 설명 마크다운 생성
- `prs/context.json`에 구조화된 분석 컨텍스트 저장

### 2단계: diff 분석
- `prs/context.json` 읽기
- `git diff main..HEAD`로 실제 코드 변경 확인
- 변경된 주요 파일을 직접 읽어 코드 의도 파악

### 3단계: PR 설명 작성
생성된 `prs/*.md`의 "## 변경 요약" 섹션을 아래 구조로 채운다:
- **변경 배경/동기**: 왜 이 변경이 필요했는지 (커밋 메시지 + 코드에서 추론)
- **주요 변경 사항**: 핵심 변경을 bullet point로 (무엇을, 왜, 어떻게)
- **주의할 점**: breaking change, 새 의존성, 설계 변경 등
- **영향 범위**: 기존 기능에 미치는 영향

### 작성 규칙
- 한국어로 작성
- 기술적이되 읽기 쉽게
- 파일 목록 나열 금지 (별도 섹션에 존재)
- 코드 변경의 "의도"에 집중

### CLI 옵션
- `--head-ref <branch>`: HEAD 대신 다른 브랜치와 비교
- `--type data|structure|both`: PR 타입 수동 지정
- `--include-worktree`: 미커밋 변경 포함 (현재 체크아웃 브랜치만)
- `--create-pr`: GitHub PR 자동 생성
- `--draft`: 드래프트 PR로 생성

## 브랜치 비교

"브랜치 비교해줘", "main과 차이" 등을 요청하면:
1. `git rev-list --left-right --count main..HEAD`
2. `git diff --name-status main..HEAD`
3. 필요 시 `python3 scripts/pr_pipeline.py --dry-run`으로 점검 실행
4. 커밋 차이, 파일 변경 요약, 리스크 노트를 정리
