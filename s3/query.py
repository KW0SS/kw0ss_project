from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable

try:
    from .uploader import _get_s3_client, _get_s3_config
except ImportError:
    from uploader import _get_s3_client, _get_s3_config

DATA_KEY_PATTERN = re.compile(
    r"^(?P<status>healthy|delisted)/"
    r"(?P<sector>[^/]+)/"
    r"(?P<ticker>\d{6})_(?P<year>\d{4})_(?P<quarter>[^/]+)\.json$"
)


@dataclass
class S3ObjectInfo:
    key: str
    size: int
    status: str | None = None
    sector: str | None = None
    ticker: str | None = None
    year: str | None = None
    quarter: str | None = None


def _get_client_and_bucket(bucket: str | None, region: str | None):
    config = _get_s3_config(bucket=bucket, region=region)
    client = _get_s3_client(config)
    return client, config["bucket"]


def _parse_data_key(key: str) -> dict[str, str] | None:
    match = DATA_KEY_PATTERN.match(key)
    if not match:
        return None
    return match.groupdict()


def _iter_objects(
    client,
    bucket: str,
    prefix: str,
    *,
    status: str | None = None,
    sector: str | None = None,
    ticker: str | None = None,
    year: str | None = None,
    quarter: str | None = None,
) -> Iterable[S3ObjectInfo]:
    paginator = client.get_paginator("list_objects_v2")
    kwargs: dict[str, Any] = {"Bucket": bucket}
    if prefix:
        kwargs["Prefix"] = prefix

    for page in paginator.paginate(**kwargs):
        for item in page.get("Contents", []):
            key = item["Key"]
            parsed = _parse_data_key(key)
            if parsed is None:
                continue
            if status and parsed["status"] != status:
                continue
            if sector and parsed["sector"] != sector:
                continue
            if ticker and parsed["ticker"] != ticker:
                continue
            if year and parsed["year"] != year:
                continue
            if quarter and parsed["quarter"] != quarter:
                continue
            yield S3ObjectInfo(
                key=key,
                size=int(item.get("Size", 0)),
                status=parsed["status"],
                sector=parsed["sector"],
                ticker=parsed["ticker"],
                year=parsed["year"],
                quarter=parsed["quarter"],
            )


def _build_prefixes(status: str | None, sector: str | None) -> list[str]:
    if status and sector:
        return [f"{status}/{sector}/"]
    if status:
        return [f"{status}/"]
    if sector:
        return [f"healthy/{sector}/", f"delisted/{sector}/"]
    return ["healthy/", "delisted/"]


def _collect_all(
    *,
    bucket: str | None,
    region: str | None,
    status: str | None = None,
    sector: str | None = None,
    ticker: str | None = None,
    year: str | None = None,
    quarter: str | None = None,
) -> list[S3ObjectInfo]:
    client, bucket_name = _get_client_and_bucket(bucket, region)
    prefixes = _build_prefixes(status, sector)
    results: list[S3ObjectInfo] = []
    for prefix in prefixes:
        for obj in _iter_objects(
            client, bucket_name, prefix,
            status=status, sector=sector, ticker=ticker,
            year=year, quarter=quarter,
        ):
            results.append(obj)
    return results


# ── 통계 함수 ─────────────────────────────────────────────────


def count_by_status(objects: list[S3ObjectInfo]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for obj in objects:
        counter[obj.status or "unknown"] += 1
    return dict(sorted(counter.items()))


def count_by_sector(objects: list[S3ObjectInfo]) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for obj in objects:
        st = obj.status or "unknown"
        sec = obj.sector or "unknown"
        result.setdefault(st, Counter())[sec] += 1
    return {
        st: dict(sorted(sectors.items()))
        for st, sectors in sorted(result.items())
    }


def count_by_year(objects: list[S3ObjectInfo]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for obj in objects:
        counter[obj.year or "unknown"] += 1
    return dict(sorted(counter.items()))


def count_by_ticker(objects: list[S3ObjectInfo]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for obj in objects:
        counter[obj.ticker or "unknown"] += 1
    return dict(sorted(counter.items()))


# ── 섹터 목록 (기존 유지) ────────────────────────────────────


def list_sectors(
    *,
    bucket: str | None,
    region: str | None,
    status: str | None,
) -> dict[str, list[str]]:
    client, bucket_name = _get_client_and_bucket(bucket, region)
    statuses = [status] if status else ["healthy", "delisted"]
    result: dict[str, list[str]] = {}
    for current_status in statuses:
        paginator = client.get_paginator("list_objects_v2")
        sector_set: set[str] = set()
        for page in paginator.paginate(
            Bucket=bucket_name,
            Prefix=f"{current_status}/",
            Delimiter="/",
        ):
            for item in page.get("CommonPrefixes", []):
                p = item.get("Prefix", "")
                sector_name = p.removeprefix(f"{current_status}/").strip("/")
                if sector_name:
                    sector_set.add(sector_name)
        result[current_status] = sorted(sector_set)
    return result


# ── 출력 헬퍼 ────────────────────────────────────────────────


def _print_json(data: Any) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2))


def _print_count_table(title: str, counts: dict[str, int]) -> None:
    total = sum(counts.values())
    print(f"\n{'=' * 50}")
    print(f" {title}  (총 {total:,}건)")
    print(f"{'=' * 50}")
    for key, cnt in counts.items():
        pct = cnt / total * 100 if total else 0
        print(f"  {key:<30s} {cnt:>8,}건  ({pct:5.1f}%)")
    print(f"{'─' * 50}")
    print(f"  {'합계':<30s} {total:>8,}건")
    print()


def _print_nested_count_table(
    title: str, data: dict[str, dict[str, int]]
) -> None:
    grand_total = sum(
        cnt for sectors in data.values() for cnt in sectors.values()
    )
    print(f"\n{'=' * 60}")
    print(f" {title}  (총 {grand_total:,}건)")
    print(f"{'=' * 60}")
    for group, counts in data.items():
        group_total = sum(counts.values())
        print(f"\n  [{group}]  소계 {group_total:,}건")
        print(f"  {'─' * 54}")
        for key, cnt in counts.items():
            pct = cnt / group_total * 100 if group_total else 0
            print(f"    {key:<28s} {cnt:>8,}건  ({pct:5.1f}%)")
    print(f"\n{'─' * 60}")
    print(f"  {'합계':<30s} {grand_total:>8,}건")
    print()


# ── CLI 커맨드 ────────────────────────────────────────────────


def _add_common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--bucket", help="S3 버킷 이름 (.env 없을 때 명시)")
    p.add_argument("--region", help="AWS 리전")
    p.add_argument("--status", choices=["healthy", "delisted"], help="정상/상폐 필터")
    p.add_argument("--sector", help="GICS 섹터 필터 (예: 'Information Technology')")
    p.add_argument("--ticker", help="6자리 종목코드 필터")
    p.add_argument("--year", help="연도 필터 (예: 2023)")
    p.add_argument("--quarter", help="분기 필터 (예: Q1, H1, Q3, ANNUAL)")
    p.add_argument("--json", dest="as_json", action="store_true", help="JSON 형식 출력")


def _collect_from_args(args: argparse.Namespace) -> list[S3ObjectInfo]:
    return _collect_all(
        bucket=args.bucket,
        region=args.region,
        status=args.status,
        sector=getattr(args, "sector", None),
        ticker=getattr(args, "ticker", None),
        year=getattr(args, "year", None),
        quarter=getattr(args, "quarter", None),
    )


def cmd_by_status(args: argparse.Namespace) -> int:
    objects = _collect_from_args(args)
    counts = count_by_status(objects)
    if args.as_json:
        _print_json(counts)
    else:
        _print_count_table("정상/상폐 기업별 데이터 건수", counts)
    return 0


def cmd_by_sector(args: argparse.Namespace) -> int:
    objects = _collect_from_args(args)
    data = count_by_sector(objects)
    if args.as_json:
        _print_json(data)
    else:
        _print_nested_count_table("상태별 GICS 섹터별 데이터 건수", data)
    return 0


def cmd_by_year(args: argparse.Namespace) -> int:
    objects = _collect_from_args(args)
    counts = count_by_year(objects)
    if args.as_json:
        _print_json(counts)
    else:
        _print_count_table("연도별 데이터 건수", counts)
    return 0


def cmd_by_ticker(args: argparse.Namespace) -> int:
    objects = _collect_from_args(args)
    counts = count_by_ticker(objects)
    if args.as_json:
        _print_json(counts)
    else:
        _print_count_table("기업코드별 데이터 건수", counts)
    return 0


def cmd_sectors(args: argparse.Namespace) -> int:
    result = list_sectors(
        bucket=args.bucket,
        region=args.region,
        status=args.status,
    )
    _print_json(result)
    return 0


# ── 파서 구성 ────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="S3 수집 데이터 현황 직접 조회 CLI"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # by-status
    p = sub.add_parser("by-status", help="정상(healthy)/상폐(delisted) 별 건수 조회")
    _add_common_args(p)
    p.set_defaults(func=cmd_by_status)

    # by-sector
    p = sub.add_parser("by-sector", help="상태별 GICS 섹터별 건수 조회")
    _add_common_args(p)
    p.set_defaults(func=cmd_by_sector)

    # by-year
    p = sub.add_parser("by-year", help="연도별 건수 조회")
    _add_common_args(p)
    p.set_defaults(func=cmd_by_year)

    # by-ticker
    p = sub.add_parser("by-ticker", help="기업코드별 건수 조회")
    _add_common_args(p)
    p.set_defaults(func=cmd_by_ticker)

    # sectors (목록만)
    p = sub.add_parser("sectors", help="status별 sector 목록 조회")
    p.add_argument("--bucket", help="S3 버킷 이름")
    p.add_argument("--region", help="AWS 리전")
    p.add_argument("--status", choices=["healthy", "delisted"])
    p.set_defaults(func=cmd_sectors)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
