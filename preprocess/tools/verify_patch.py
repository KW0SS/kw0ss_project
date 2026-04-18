"""
account_mapper 패턴 수정 전후 효과 검증 스크립트.

수정 이력
─────────
v2: after 매핑을 하드코딩 패턴 대신 account_mapper.py 직접 임포트로 변경.
    extract_standard_items의 로직 수정(중단영업 합산 등) 효과까지 반영됨.

사용법:
    python verify_patch.py                          # 전체 비교
    python verify_patch.py --sample 010470          # 특정 기업코드만
    python verify_patch.py --failures-only          # 여전히 실패하는 케이스 상세 출력
    python verify_patch.py --raw-root data/raw --workers 8
"""

import json
import re
import sys
import argparse
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict
from tqdm import tqdm

# account_mapper.py가 프로젝트 루트에 있다고 가정
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from account_mapper import extract_standard_items, ACCOUNT_PATTERNS

TARGET_KEYS = ["revenue", "operating_income", "net_income"]

# ── 수정 전 패턴 (before 기준선) ────────────────────────────
PATTERNS_BEFORE = [
    ("revenue",          "IS",  r"^매출액$|^매출$|^수익\s*\(매출액\)$|^영업\s*수익$|^수익$"),
    ("operating_income", "IS",  r"영업\s*이익|영업\s*손익"),
    ("net_income",       "IS",  r"당기\s*순이익|당기순이익|당기\s*순\s*손익"),
    ("revenue",          "CIS", r"^매출액$|^매출$|^수익\s*\(매출액\)$|^영업\s*수익$|^수익$"),
    ("operating_income", "CIS", r"영업\s*이익|영업\s*손익"),
    ("net_income",       "CIS", r"당기\s*순이익|당기순이익|당기\s*순\s*손익"),
]

# after 역추적용 (어떤 account_nm이 새로 잡혔는지 확인)
_COMPILED_AFTER = [(k, sj, re.compile(p)) for k, sj, p in ACCOUNT_PATTERNS]


def _match_before(data: list[dict]) -> dict:
    """수정 전 패턴으로만 매핑 (로직 변경 없음)."""
    compiled = [(k, sj, re.compile(p)) for k, sj, p in PATTERNS_BEFORE]
    matched = {}
    for item in data:
        nm = (item.get("account_nm") or "").strip()
        sj = (item.get("sj_div")     or "").strip()
        for std_key, filter_sj, regex in compiled:
            if std_key in matched:
                continue
            if filter_sj and sj != filter_sj:
                continue
            if regex.search(nm):
                matched[std_key] = {
                    "account_nm": nm,
                    "sj_div":     sj,
                    "thstrm":     item.get("thstrm_amount"),
                    "frmtrm":     item.get("frmtrm_amount"),
                }
                break
    return matched


def _find_matched_nm(data: list[dict], key: str) -> str:
    """after에서 특정 키가 어떤 account_nm으로 잡혔는지 역추적.
    패턴으로 못 찾으면 로직(계속영업+중단영업 합산)으로 잡힌 것."""
    for item in data:
        nm = (item.get("account_nm") or "").strip()
        sj = (item.get("sj_div")     or "").strip()
        for std_key, filter_sj, regex in _COMPILED_AFTER:
            if std_key != key:
                continue
            if filter_sj and sj != filter_sj:
                continue
            if regex.search(nm):
                return nm
    return "[계속영업+중단영업 합산]"


def check_file(json_path: Path) -> dict:
    """단일 파일에 대해 before/after 매핑 결과를 모두 반환."""
    try:
        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return {"path": str(json_path), "error": str(e)}

    before = _match_before(data)
    after  = extract_standard_items(data)  # account_mapper.py 직접 사용

    failed_before = [k for k in TARGET_KEYS if k not in before]
    failed_after  = [k for k in TARGET_KEYS if k not in after]

    # 변화 분류
    if not failed_before and not failed_after:
        status = "OK"           # 원래부터 정상
    elif failed_before and not failed_after:
        status = "FIXED"        # 수정으로 완전 해소
    elif failed_before and failed_after and len(failed_after) < len(failed_before):
        status = "PARTIAL"      # 일부만 해소
    elif failed_before == failed_after:
        status = "UNCHANGED"    # 수정해도 여전히 동일하게 실패
    else:
        status = "REGRESSION"   # 수정 후 오히려 실패 증가 (심각)

    # 패치로 새로 잡힌 account_nm 역추적
    newly_matched = {
        k: _find_matched_nm(data, k)
        for k in TARGET_KEYS
        if k not in before and k in after
    }

    return {
        "path":          str(json_path),
        "corp":          json_path.parent.name,
        "category":      json_path.parent.parent.name,
        "split":         json_path.parent.parent.parent.name,
        "status":        status,
        "failed_before": failed_before,
        "failed_after":  failed_after,
        "newly_matched": newly_matched,
    }


def run_verify(
    raw_root: str = "data/raw",
    workers: int = 8,
    sample_corp: str | None = None,
    failures_only: bool = False,
    save_result: str = "verify_result.json",
):
    root = Path(raw_root)
    all_json = list(root.rglob("*.json"))

    if sample_corp:
        all_json = [p for p in all_json if sample_corp in str(p)]
        print(f"필터링 후 대상: {len(all_json)}개 ({sample_corp})\n")
    else:
        print(f"총 대상: {len(all_json)}개, workers: {workers}\n")

    # ── 병렬 실행 ─────────────────────────────────────────────
    results = []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(check_file, p): p for p in all_json}
        for future in tqdm(as_completed(futures), total=len(all_json), desc="비교 중"):
            results.append(future.result())

    # ── 집계 ──────────────────────────────────────────────────
    counter   = defaultdict(int)
    newly_nms = defaultdict(lambda: defaultdict(int))

    for r in results:
        if "error" in r:
            counter["ERROR"] += 1
            continue
        counter[r["status"]] += 1
        for k, nm in r.get("newly_matched", {}).items():
            newly_nms[k][nm] += 1

    total = len(results)

    # ── 요약 출력 ─────────────────────────────────────────────
    print("\n" + "=" * 55)
    print(f"{'상태':<12} {'건수':>7}  {'비율':>6}  {'설명'}")
    print("-" * 55)
    descriptions = {
        "OK":         "원래부터 정상",
        "FIXED":      "패치로 완전 해소  ✅",
        "PARTIAL":    "일부만 해소       ⚠️",
        "UNCHANGED":  "여전히 실패       ❌",
        "REGRESSION": "패치 후 악화      🚨",
        "ERROR":      "파싱 오류",
    }
    for status in ["OK", "FIXED", "PARTIAL", "UNCHANGED", "REGRESSION", "ERROR"]:
        cnt = counter[status]
        if cnt == 0:
            continue
        pct = cnt / total * 100
        print(f"{status:<12} {cnt:>7}  {pct:>5.1f}%  {descriptions[status]}")
    print("=" * 55)

    # before/after 실패 건수 비교
    total_fail_before = sum(
        len(r["failed_before"]) for r in results if "error" not in r
    )
    total_fail_after = sum(
        len(r["failed_after"]) for r in results if "error" not in r
    )
    print(f"\n키 단위 실패 건수: {total_fail_before} → {total_fail_after} "
          f"({total_fail_before - total_fail_after:+d})")

    # 새로 잡힌 account_nm 빈도
    if any(newly_nms.values()):
        print("\n[패치로 새로 매핑된 account_nm 빈도 TOP 15]")
        for key in TARGET_KEYS:
            if not newly_nms[key]:
                continue
            print(f"\n  ── {key} ──")
            for nm, cnt in sorted(newly_nms[key].items(), key=lambda x: -x[1])[:15]:
                print(f"    {cnt:4d}회  {nm}")

    # UNCHANGED / REGRESSION / PARTIAL 상세
    problem = [
        r for r in results
        if r.get("status") in ("UNCHANGED", "REGRESSION", "PARTIAL")
    ]
    if failures_only and problem:
        print(f"\n[미해소 케이스 상세 - {len(problem)}개 중 최대 30개]")
        for r in problem[:30]:
            print(f"\n  {r['status']} | {r['path']}")
            print(f"  before 실패: {r['failed_before']}")
            print(f"  after  실패: {r['failed_after']}")

    # 결과 저장
    if save_result:
        with open(save_result, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"\n전체 결과 저장: {save_result}")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="account_mapper 패턴 수정 전후 비교")
    parser.add_argument("--raw-root",      default="data/raw",           help="raw JSON 루트 경로")
    parser.add_argument("--workers",       type=int, default=8,          help="병렬 프로세스 수")
    parser.add_argument("--sample",        default=None,                 help="특정 기업코드 필터")
    parser.add_argument("--failures-only", action="store_true",          help="미해소 케이스 상세 출력")
    parser.add_argument("--save-result",   default="verify_result.json", help="결과 저장 경로")
    args = parser.parse_args()

    run_verify(
        raw_root=args.raw_root,
        workers=args.workers,
        sample_corp=args.sample,
        failures_only=args.failures_only,
        save_result=args.save_result,
    )