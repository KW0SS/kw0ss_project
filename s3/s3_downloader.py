"""S3에서 재무제표 원본 JSON 데이터 병렬 다운로드.

S3 디렉터리 구조
─────────────────
s3://{bucket}/
  ├── healthy/
  │   └── {gics_sector}/
  │       ├── 005930_2023_Q1.json
  │       └── ...
  └── delisted/
      └── {gics_sector}/
          ├── 019440_2023_ANNUAL.json
          └── ...

필요한 환경변수 (.env)
─────────────────────
  S3_ACCESS_KEY    – AWS Access Key ID
  S3_PRIVATE_KEY   – AWS Secret Access Key
  S3_BUCKET_NAME   – S3 버킷 이름
  S3_REGION        – (선택) AWS 리전 (기본: ap-northeast-2)

생성되는 메타데이터 파일 (data/meta/)
──────────────────────────────────────
  s3_snapshot_{timestamp}.json   – 다운로드 시점 S3 건수 현황 (상태/섹터/연도별)
  download_log_{timestamp}.json  – 실행 조건 + 다운로드 결과 기록
                                   (워커 수, 소요시간, 실패 key 목록 포함)

사용 예시
─────────
# 전체 다운로드 (기본 워커 5개)
python s3_downloader.py download

# 워커 10개로 빠르게
python s3_downloader.py download --workers 10

# 정상 기업만
python s3_downloader.py download --status healthy

# 특정 섹터만
python s3_downloader.py download --sector "Information Technology"

# 저장 경로 지정
python s3_downloader.py download --output-dir data/raw/

# 이미 있는 파일 덮어쓰기
python s3_downloader.py download --force

# 다운로드 없이 건수만 확인 + 스냅샷 저장
python s3_downloader.py count
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    from .uploader import _get_s3_client, _get_s3_config
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from uploader import _get_s3_client, _get_s3_config

try:
    from .query import _collect_all, S3ObjectInfo
except ImportError:
    from query import _collect_all, S3ObjectInfo

# ── 경로 설정 ─────────────────────────────────────────────────
PROJECT_ROOT    = Path(__file__).resolve().parent.parent / "preprocess"
DEFAULT_RAW_DIR = PROJECT_ROOT / "data" / "raw"
META_DIR        = PROJECT_ROOT / "data" / "meta"

# ── KST 타임존 ────────────────────────────────────────────────
KST = timezone(timedelta(hours=9))


def _now_kst() -> datetime:
    return datetime.now(KST)


def _timestamp() -> str:
    return _now_kst().strftime("%Y%m%d_%H%M%S")


# ══════════════════════════════════════════════════════════════
# 메타데이터 관련 함수
# ══════════════════════════════════════════════════════════════

def _build_snapshot(objects: list[S3ObjectInfo], bucket_name: str) -> dict:
    """
    S3 객체 목록으로부터 스냅샷 딕셔너리 생성.

    저장 내용:
      - 스냅샷 생성 일시 (KST)
      - S3 버킷 이름
      - 총 파일 수
      - 상태별 건수 (healthy / delisted)
      - 섹터별 건수 (상태 포함)
      - 연도별 건수
      - 분기별 건수
    """
    by_status:  Counter = Counter()
    by_sector:  Counter = Counter()
    by_year:    Counter = Counter()
    by_quarter: Counter = Counter()

    for obj in objects:
        by_status[obj.status or "unknown"] += 1
        by_sector[f"{obj.status}/{obj.sector}"] += 1
        by_year[obj.year or "unknown"] += 1
        by_quarter[obj.quarter or "unknown"] += 1

    return {
        "snapshot_at": _now_kst().isoformat(),
        "bucket":      bucket_name,
        "total_files": len(objects),
        "by_status":   dict(sorted(by_status.items())),
        "by_sector":   dict(sorted(by_sector.items())),
        "by_year":     dict(sorted(by_year.items())),
        "by_quarter":  dict(sorted(by_quarter.items())),
    }


def _save_snapshot(snapshot: dict) -> Path:
    """
    [메타데이터] S3 스냅샷을 data/meta/s3_snapshot_{timestamp}.json 에 저장.

    언제 생성되나:
      - download 커맨드 실행 시 (다운로드 시작 전)
      - count 커맨드 실행 시
    왜 필요한가:
      - "언제 기준으로 받은 데이터인지" 나중에 추적 가능
      - 팀원이 같은 데이터를 다시 받을 때 건수 비교 기준이 됨
    """
    META_DIR.mkdir(parents=True, exist_ok=True)
    path = META_DIR / f"s3_snapshot_{_timestamp()}.json"
    path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[메타] S3 스냅샷 저장: {path}", file=sys.stderr)
    return path


def _save_download_log(
    *,
    filters: dict,
    output_dir: Path,
    total_found: int,
    downloaded: int,
    skipped: int,
    failed: int,
    failed_keys: list[str],
    snapshot_path: Path | None,
    elapsed_sec: float,
    workers: int,
    max_retries: int,
) -> Path:
    """
    [메타데이터] 다운로드 실행 결과를 data/meta/download_log_{timestamp}.json 에 저장.

    저장 내용:
      - 실행 일시 (KST)
      - 적용한 필터 조건 (status, sector, ticker, year, quarter)
      - 병렬 워커 수, 최대 재시도 횟수
      - 로컬 저장 경로
      - S3에서 발견된 총 파일 수
      - 저장 성공 / 스킵 / 실패 건수 + 성공률
      - 실패한 S3 key 목록 (재시도 시 활용)
      - 연결된 스냅샷 파일 경로
      - 실제 소요 시간(초)

    왜 필요한가:
      - 어떤 조건으로 받았는지 재현 가능
      - 실패한 파일만 골라서 재시도 가능
      - 전처리 파트에서 "원본이 몇 건이었는지" 기준점이 됨
    """
    META_DIR.mkdir(parents=True, exist_ok=True)
    log = {
        "log_type":    "download",
        "executed_at": _now_kst().isoformat(),
        "elapsed_sec": round(elapsed_sec, 2),
        "elapsed_str": f"{int(elapsed_sec // 60)}분 {int(elapsed_sec % 60)}초",
        "settings": {
            "workers":     workers,
            "max_retries": max_retries,
            "filters":     filters,
            "output_dir":  str(output_dir.resolve()),
        },
        "s3_total_found": total_found,
        "result": {
            "downloaded":   downloaded,
            "skipped":      skipped,
            "failed":       failed,
            "success_rate": f"{downloaded / total_found:.1%}" if total_found else "0%",
        },
        "failed_keys":   failed_keys,
        "snapshot_file": str(snapshot_path) if snapshot_path else None,
    }
    path = META_DIR / f"download_log_{_timestamp()}.json"
    path.write_text(json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[메타] 다운로드 로그 저장: {path}", file=sys.stderr)
    return path


# ══════════════════════════════════════════════════════════════
# 단일 파일 다운로드 (재시도 포함)
# ══════════════════════════════════════════════════════════════

def _print_progress(counter: dict, total: int) -> None:
    """진행률을 한 줄로 덮어쓰기 출력."""
    done = counter["done"]
    pct  = done / total * 100 if total else 0
    print(
        f"\r진행: {done}/{total} ({pct:.1f}%) | "
        f"저장 {counter['downloaded']} / 스킵 {counter['skipped']} / 실패 {counter['failed']}",
        end="", file=sys.stderr,
    )


def _download_one(
    client,
    bucket_name: str,
    obj: S3ObjectInfo,
    save_dir: Path,
    force: bool,
    max_retries: int,
    progress_lock: threading.Lock,
    counter: dict,
    total: int,
) -> tuple[str, Path | None]:
    """
    파일 1개 다운로드. 실패 시 max_retries 회까지 자동 재시도.

    재시도 간격: 0.5초 → 1.0초 → 1.5초 (선형 증가)

    Returns:
        ("ok" | "skip" | "fail", 로컬경로 or None)
    """
    local_path = save_dir / obj.key
    local_path.parent.mkdir(parents=True, exist_ok=True)

    # 이미 존재하면 스킵
    if local_path.exists() and not force:
        with progress_lock:
            counter["skipped"] += 1
            counter["done"] += 1
            _print_progress(counter, total)
        return "skip", local_path

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            response = client.get_object(Bucket=bucket_name, Key=obj.key)
            body = response["Body"].read()
            local_path.write_bytes(body)
            with progress_lock:
                counter["downloaded"] += 1
                counter["done"] += 1
                _print_progress(counter, total)
            return "ok", local_path
        except Exception as e:
            last_err = e
            if attempt < max_retries:
                time.sleep(0.5 * attempt)

    with progress_lock:
        counter["failed"] += 1
        counter["done"] += 1
        _print_progress(counter, total)
    print(f"\n  [실패] {obj.key}: {last_err}", file=sys.stderr)
    return "fail", None


# ══════════════════════════════════════════════════════════════
# 병렬 다운로드 메인 함수
# ══════════════════════════════════════════════════════════════

def download_from_s3(
    *,
    output_dir: Path | None = None,
    bucket: str | None = None,
    region: str | None = None,
    status: str | None = None,
    sector: str | None = None,
    ticker: str | None = None,
    year: str | None = None,
    quarter: str | None = None,
    force: bool = False,
    workers: int = 5,
    max_retries: int = 3,
) -> list[Path]:
    """
    S3에서 조건에 맞는 JSON 파일을 로컬로 병렬 다운로드.

    로컬 저장 경로: {output_dir}/{status}/{sector}/{ticker}_{year}_{quarter}.json

    병렬 처리:
      - workers 개의 스레드가 동시에 파일을 다운로드
      - 각 파일은 실패 시 max_retries 회까지 자동 재시도
      - S3 Rate Limit 안전 범위: workers 10 이하 권장

    실행 시 자동으로 두 가지 메타데이터 파일 생성:
      1. s3_snapshot_{timestamp}.json  – 다운로드 전 S3 현황
      2. download_log_{timestamp}.json – 실행 조건 + 결과

    Returns:
        다운로드된 로컬 파일 경로 리스트
    """
    save_dir   = output_dir or DEFAULT_RAW_DIR
    start_time = time.time()

    config      = _get_s3_config(bucket=bucket, region=region)
    bucket_name = config["bucket"]

    # ── S3 객체 목록 수집 ─────────────────────────────────────
    print(f"S3 객체 목록 조회 중 (s3://{bucket_name}/)...", file=sys.stderr)
    objects: list[S3ObjectInfo] = _collect_all(
        bucket=bucket, region=region,
        status=status, sector=sector,
        ticker=ticker, year=year, quarter=quarter,
    )

    if not objects:
        print("다운로드할 파일이 없습니다.", file=sys.stderr)
        return []

    total = len(objects)
    print(f"총 {total:,}개 파일 발견 | 워커 {workers}개 | 재시도 최대 {max_retries}회\n", file=sys.stderr)

    # ── [메타] S3 스냅샷 저장 (다운로드 시작 전) ──────────────
    snapshot      = _build_snapshot(objects, bucket_name)
    snapshot_path = _save_snapshot(snapshot)
    print("", file=sys.stderr)

    # ── 병렬 다운로드 ─────────────────────────────────────────
    # boto3 클라이언트는 스레드 간 공유 불가 → _download_one 안에서 각각 생성
    progress_lock    = threading.Lock()
    counter          = {"done": 0, "downloaded": 0, "skipped": 0, "failed": 0}
    downloaded_paths: list[Path] = []
    failed_keys:      list[str]  = []
    results_lock     = threading.Lock()

    def _task(obj: S3ObjectInfo):
        client = _get_s3_client(config)  # 스레드마다 독립 클라이언트
        return _download_one(
            client, bucket_name, obj, save_dir, force,
            max_retries, progress_lock, counter, total,
        )

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_task, obj): obj for obj in objects}
        for future in as_completed(futures):
            result, path = future.result()
            with results_lock:
                if result == "ok" and path:
                    downloaded_paths.append(path)
                elif result == "fail":
                    failed_keys.append(futures[future].key)

    elapsed = time.time() - start_time
    print(
        f"\n\n✅ 완료: {len(downloaded_paths)}개 저장 / "
        f"{counter['skipped']}개 스킵 / {len(failed_keys)}개 실패 | "
        f"소요: {int(elapsed // 60)}분 {int(elapsed % 60)}초",
        file=sys.stderr,
    )

    # ── [메타] 다운로드 로그 저장 ─────────────────────────────
    _save_download_log(
        filters={
            "status": status, "sector": sector, "ticker": ticker,
            "year": year, "quarter": quarter, "force": force,
        },
        output_dir=save_dir,
        total_found=total,
        downloaded=len(downloaded_paths),
        skipped=counter["skipped"],
        failed=len(failed_keys),
        failed_keys=failed_keys,
        snapshot_path=snapshot_path,
        elapsed_sec=elapsed,
        workers=workers,
        max_retries=max_retries,
    )

    return downloaded_paths


# ══════════════════════════════════════════════════════════════
# 건수 확인
# ══════════════════════════════════════════════════════════════

def count_s3_objects(
    *,
    bucket: str | None = None,
    region: str | None = None,
    status: str | None = None,
    sector: str | None = None,
    ticker: str | None = None,
    year: str | None = None,
    quarter: str | None = None,
) -> None:
    """
    다운로드 없이 S3 객체 건수만 출력.
    실행 시 s3_snapshot_{timestamp}.json 도 함께 저장됩니다.
    """
    config      = _get_s3_config(bucket=bucket, region=region)
    bucket_name = config["bucket"]

    print("S3 객체 목록 조회 중...", file=sys.stderr)
    objects = _collect_all(
        bucket=bucket, region=region,
        status=status, sector=sector,
        ticker=ticker, year=year, quarter=quarter,
    )

    snapshot = _build_snapshot(objects, bucket_name)
    _save_snapshot(snapshot)

    print(f"\n총 파일 수: {len(objects):,}개\n")

    print("── 상태별 ──")
    for k, v in sorted(snapshot["by_status"].items()):
        print(f"  {k:<12} {v:>6,}개")

    print("\n── 연도별 ──")
    for k, v in sorted(snapshot["by_year"].items()):
        print(f"  {k:<10} {v:>6,}개")

    print("\n── 분기별 ──")
    for k, v in sorted(snapshot["by_quarter"].items()):
        print(f"  {k:<10} {v:>6,}개")

    print("\n── 섹터별 ──")
    for k, v in sorted(snapshot["by_sector"].items()):
        print(f"  {k:<50} {v:>6,}개")


# ── CLI ───────────────────────────────────────────────────────

def _add_filter_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--bucket",  help="S3 버킷 이름 (.env 없을 때 명시)")
    p.add_argument("--region",  help="AWS 리전")
    p.add_argument("--status",  choices=["healthy", "delisted"], help="정상/상폐 필터")
    p.add_argument("--sector",  help="GICS 섹터 필터 (예: 'Information Technology')")
    p.add_argument("--ticker",  help="6자리 종목코드 필터")
    p.add_argument("--year",    help="연도 필터 (예: 2023)")
    p.add_argument("--quarter", help="분기 필터 (예: Q1, H1, Q3, ANNUAL)")


def cmd_download(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir) if args.output_dir else None
    downloaded = download_from_s3(
        output_dir=output_dir,
        bucket=args.bucket,
        region=args.region,
        status=args.status,
        sector=args.sector,
        ticker=args.ticker,
        year=args.year,
        quarter=args.quarter,
        force=args.force,
        workers=args.workers,
        max_retries=args.max_retries,
    )
    if downloaded:
        print(f"\n저장 위치: {(output_dir or DEFAULT_RAW_DIR).resolve()}")
    return 0


def cmd_count(args: argparse.Namespace) -> int:
    count_s3_objects(
        bucket=args.bucket,
        region=args.region,
        status=args.status,
        sector=args.sector,
        ticker=args.ticker,
        year=args.year,
        quarter=args.quarter,
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="S3 재무제표 JSON 병렬 다운로드 도구",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # download
    p = sub.add_parser("download", help="S3 데이터 로컬 병렬 다운로드")
    _add_filter_args(p)
    p.add_argument("--output-dir", "-o", help="로컬 저장 디렉터리 (기본: data/raw/)")
    p.add_argument("--workers", "-w", type=int, default=5,
                   help="동시 다운로드 워커 수 (기본: 5, 권장 최대: 10)")
    p.add_argument("--max-retries", type=int, default=3,
                   help="파일당 최대 재시도 횟수 (기본: 3)")
    p.add_argument("--force", action="store_true",
                   help="이미 존재하는 파일도 덮어쓰기")
    p.set_defaults(func=cmd_download)

    # count
    p = sub.add_parser("count", help="다운로드 없이 S3 파일 건수만 확인 + 스냅샷 저장")
    _add_filter_args(p)
    p.set_defaults(func=cmd_count)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())