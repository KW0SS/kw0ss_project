from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .s3_uploader import _get_s3_client, _get_s3_config

KEY_PATTERN = re.compile(
    r"^(?P<status>healthy|delisted)/"
    r"(?P<sector>[^/]+)/"
    r"(?P<ticker>\d{6})_(?P<year>\d{4})_(?P<quarter>[^/]+)\.json$"
)


@dataclass
class S3ObjectInfo:
    key: str
    size: int
    last_modified: str
    status: str | None = None
    sector: str | None = None
    ticker: str | None = None
    year: str | None = None
    quarter: str | None = None

    @property
    def uri(self) -> str:
        return self.key


def _parse_key(key: str) -> dict[str, str] | None:
    match = KEY_PATTERN.match(key)
    if not match:
        return None
    return match.groupdict()


def _build_prefix(status: str | None, sector: str | None, prefix: str | None) -> str:
    if prefix:
        return prefix.strip("/")
    parts = [part for part in [status, sector] if part]
    return "/".join(parts)


def _iter_objects(client, bucket: str, prefix: str) -> Iterable[S3ObjectInfo]:
    paginator = client.get_paginator("list_objects_v2")
    kwargs: dict[str, Any] = {"Bucket": bucket}
    if prefix:
        kwargs["Prefix"] = prefix

    for page in paginator.paginate(**kwargs):
        for item in page.get("Contents", []):
            key = item["Key"]
            parsed = _parse_key(key) or {}
            yield S3ObjectInfo(
                key=key,
                size=int(item.get("Size", 0)),
                last_modified=item.get("LastModified").isoformat()
                if item.get("LastModified")
                else "",
                status=parsed.get("status"),
                sector=parsed.get("sector"),
                ticker=parsed.get("ticker"),
                year=parsed.get("year"),
                quarter=parsed.get("quarter"),
            )


def _matches_filters(
    obj: S3ObjectInfo,
    status: str | None,
    sector: str | None,
    ticker: str | None,
    year: str | None,
    quarter: str | None,
) -> bool:
    if status and obj.status != status:
        return False
    if sector and obj.sector != sector:
        return False
    if ticker and obj.ticker != ticker:
        return False
    if year and obj.year != year:
        return False
    if quarter and obj.quarter != quarter:
        return False
    return True


def find_objects(
    *,
    bucket: str,
    region: str | None,
    status: str | None,
    sector: str | None,
    ticker: str | None,
    year: str | None,
    quarter: str | None,
    prefix: str | None,
    limit: int,
) -> list[S3ObjectInfo]:
    config = _get_s3_config(bucket=bucket, region=region)
    client = _get_s3_client(config)

    prefixes: list[str]
    if prefix:
        prefixes = [_build_prefix(status, sector, prefix)]
    elif status:
        prefixes = [_build_prefix(status, sector, None)]
    else:
        prefixes = [
            _build_prefix("healthy", sector, None),
            _build_prefix("delisted", sector, None),
        ]

    matched: list[S3ObjectInfo] = []
    for current_prefix in prefixes:
        for obj in _iter_objects(client, config["bucket"], current_prefix):
            if not _matches_filters(obj, status, sector, ticker, year, quarter):
                continue
            matched.append(obj)
            if len(matched) >= limit:
                return matched
    return matched


def list_sectors(*, bucket: str, region: str | None, status: str | None) -> dict[str, list[str]]:
    config = _get_s3_config(bucket=bucket, region=region)
    client = _get_s3_client(config)

    statuses = [status] if status else ["healthy", "delisted"]
    result: dict[str, list[str]] = {}
    for current_status in statuses:
        paginator = client.get_paginator("list_objects_v2")
        prefixes: set[str] = set()
        for page in paginator.paginate(
            Bucket=config["bucket"],
            Prefix=f"{current_status}/",
            Delimiter="/",
        ):
            for item in page.get("CommonPrefixes", []):
                prefix = item.get("Prefix", "")
                sector_name = prefix.removeprefix(f"{current_status}/").strip("/")
                if sector_name:
                    prefixes.add(sector_name)
        result[current_status] = sorted(prefixes)
    return result


def read_object(
    *,
    bucket: str,
    region: str | None,
    key: str,
) -> tuple[str, Any]:
    config = _get_s3_config(bucket=bucket, region=region)
    client = _get_s3_client(config)
    response = client.get_object(Bucket=config["bucket"], Key=key)
    raw = response["Body"].read().decode("utf-8")
    try:
        return raw, json.loads(raw)
    except json.JSONDecodeError:
        return raw, None


def _print_table(objects: list[S3ObjectInfo], bucket: str) -> None:
    if not objects:
        print("조회 결과가 없습니다.")
        return

    print("status\tsector\tticker\tyear\tquarter\tsize\tlast_modified\turi")
    for obj in objects:
        print(
            f"{obj.status or '-'}\t"
            f"{obj.sector or '-'}\t"
            f"{obj.ticker or '-'}\t"
            f"{obj.year or '-'}\t"
            f"{obj.quarter or '-'}\t"
            f"{obj.size}\t"
            f"{obj.last_modified or '-'}\t"
            f"s3://{bucket}/{obj.key}"
        )


def cmd_list(args: argparse.Namespace) -> int:
    objects = find_objects(
        bucket=args.bucket,
        region=args.region,
        status=args.status,
        sector=args.sector,
        ticker=args.ticker,
        year=args.year,
        quarter=args.quarter,
        prefix=args.prefix,
        limit=args.limit,
    )
    _print_table(objects, args.bucket or _get_s3_config(region=args.region)["bucket"])
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    bucket_name = args.bucket or _get_s3_config(region=args.region)["bucket"]

    key = args.key
    if not key:
        objects = find_objects(
            bucket=bucket_name,
            region=args.region,
            status=args.status,
            sector=args.sector,
            ticker=args.ticker,
            year=args.year,
            quarter=args.quarter,
            prefix=args.prefix,
            limit=2,
        )
        if not objects:
            print("조건에 맞는 S3 객체가 없습니다.")
            return 1
        if len(objects) > 1:
            print("조건에 맞는 객체가 여러 개입니다. --key 또는 더 구체적인 필터를 사용하세요.")
            _print_table(objects, bucket_name)
            return 1
        key = objects[0].key

    raw, parsed = read_object(bucket=bucket_name, region=args.region, key=key)
    print(f"uri: s3://{bucket_name}/{key}")

    if args.raw or parsed is None:
        print(raw)
        return 0

    if isinstance(parsed, list):
        print(f"record_count: {len(parsed)}")
        preview = parsed[: args.max_items]
        print(json.dumps(preview, ensure_ascii=False, indent=2))
        return 0

    if isinstance(parsed, dict):
        print(json.dumps(parsed, ensure_ascii=False, indent=2))
        return 0

    print(raw)
    return 0


def cmd_sectors(args: argparse.Namespace) -> int:
    bucket_name = args.bucket or _get_s3_config(region=args.region)["bucket"]
    result = list_sectors(
        bucket=bucket_name,
        region=args.region,
        status=args.status,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="S3에 저장된 수집 데이터를 조회하는 CLI"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common_filters(subparser: argparse.ArgumentParser, *, allow_key: bool = False) -> None:
        subparser.add_argument("--bucket", help="S3 버킷 이름 (.env 없을 때 명시)")
        subparser.add_argument("--region", help="AWS 리전")
        subparser.add_argument("--status", choices=["healthy", "delisted"])
        subparser.add_argument("--sector", help="예: Information Technology")
        subparser.add_argument("--ticker", help="6자리 종목코드")
        subparser.add_argument("--year", help="예: 2023")
        subparser.add_argument("--quarter", help="예: Q1, H1, Q3, ANNUAL")
        subparser.add_argument("--prefix", help="직접 S3 prefix 지정")
        if allow_key:
            subparser.add_argument("--key", help="직접 S3 key 지정")

    list_p = sub.add_parser("list", help="조건에 맞는 S3 객체 목록 조회")
    add_common_filters(list_p)
    list_p.add_argument("--limit", type=int, default=50, help="최대 조회 개수")
    list_p.set_defaults(func=cmd_list)

    show_p = sub.add_parser("show", help="S3 객체 내용 미리보기")
    add_common_filters(show_p, allow_key=True)
    show_p.add_argument("--max-items", type=int, default=3, help="배열 JSON 미리보기 개수")
    show_p.add_argument("--raw", action="store_true", help="원본 본문 전체 출력")
    show_p.set_defaults(func=cmd_show)

    sectors_p = sub.add_parser("sectors", help="status별 sector 목록 조회")
    sectors_p.add_argument("--bucket", help="S3 버킷 이름 (.env 없을 때 명시)")
    sectors_p.add_argument("--region", help="AWS 리전")
    sectors_p.add_argument("--status", choices=["healthy", "delisted"])
    sectors_p.set_defaults(func=cmd_sectors)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
