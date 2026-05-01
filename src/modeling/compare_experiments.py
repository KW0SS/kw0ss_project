"""
실험 간 비교 도구.

results/ 하위의 exp_* 디렉터리를 스캔하여
실험 조건과 성능을 한눈에 비교한다.

사용법:
    python -m src.modeling.compare_experiments
    python -m src.modeling.compare_experiments --metric pr_auc --split test
    python -m src.modeling.compare_experiments --horizon 12
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = PROJECT_ROOT / "results"

METRIC_NAMES = ["pr_auc", "f1", "precision", "recall", "roc_auc"]


# ---------------------------------------------------------------------------
# 실험 스캔
# ---------------------------------------------------------------------------


def scan_experiments(results_dir: Path | None = None) -> list[dict]:
    """results/exp_* 디렉터리를 스캔하여 실험 목록을 반환한다."""
    base = results_dir or RESULTS_DIR
    experiments = []

    for exp_dir in sorted(base.glob("exp_*")):
        if not exp_dir.is_dir():
            continue

        meta_path = exp_dir / "experiment.json"
        if not meta_path.exists():
            continue

        with open(meta_path, encoding="utf-8") as f:
            meta = json.load(f)

        # 개별 모델 결과 수집
        model_results = []
        for json_path in sorted(exp_dir.glob("*_H*.json")):
            if json_path.name == "experiment.json":
                continue
            with open(json_path, encoding="utf-8") as f:
                model_results.append(json.load(f))

        experiments.append({
            "dir": exp_dir,
            "meta": meta,
            "model_results": model_results,
        })

    return experiments


# ---------------------------------------------------------------------------
# 비교 테이블 생성
# ---------------------------------------------------------------------------


def build_cross_experiment_table(
    experiments: list[dict],
    split: str = "test",
    horizon: int | None = None,
) -> pd.DataFrame:
    """실험 간 비교 테이블을 생성한다.

    Args:
        split: "valid" 또는 "test"
        horizon: 특정 horizon만 필터. None이면 전체
    """
    rows = []
    for exp in experiments:
        meta = exp["meta"]
        exp_id = meta.get("experiment_id", exp["dir"].name)
        exp_name = meta.get("name", "")
        preprocess = meta.get("preprocessing", {})

        for result in exp["model_results"]:
            h = result.get("horizon")
            if horizon is not None and h != horizon:
                continue

            metrics = result.get(split, {})
            row = {
                "experiment": exp_id,
                "exp_name": exp_name,
                "preprocess": _preprocess_tag(preprocess),
                "model": result.get("model_name"),
                "horizon": h,
                "threshold": result.get("threshold"),
            }
            for m in METRIC_NAMES:
                row[m] = metrics.get(m)
            rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("pr_auc", ascending=False).reset_index(drop=True)
    return df


def _preprocess_tag(preprocess: dict) -> str:
    """전처리 조건을 짧은 태그로 변환한다."""
    parts = ["clip"]
    if preprocess.get("winsorize"):
        parts.append("winsor")
    if preprocess.get("robust_scale"):
        parts.append("robust")
    return "+".join(parts)


# ---------------------------------------------------------------------------
# Best-per-experiment 요약
# ---------------------------------------------------------------------------


def best_per_experiment(
    experiments: list[dict],
    split: str = "test",
    horizon: int | None = None,
    metric: str = "pr_auc",
) -> pd.DataFrame:
    """각 실험의 best 모델만 뽑아 비교한다."""
    df = build_cross_experiment_table(experiments, split, horizon)
    if df.empty:
        return df

    idx = df.groupby("experiment")[metric].idxmax()
    return df.loc[idx].reset_index(drop=True)


# ---------------------------------------------------------------------------
# 출력
# ---------------------------------------------------------------------------


def print_table(df: pd.DataFrame, title: str = "") -> None:
    """비교 테이블을 포맷팅하여 출력한다."""
    if df.empty:
        print("결과 없음")
        return

    if title:
        print(f"\n{'='*80}")
        print(f"  {title}")
        print(f"{'='*80}")

    header = (f"{'Experiment':<12} {'Name':<18} {'Preproc':<14} "
              f"{'Model':<6} {'H':>3} {'Thr':>6}")
    header += "".join(f"{m:>10}" for m in METRIC_NAMES)
    print(header)
    print("-" * len(header))

    for _, row in df.iterrows():
        line = (f"{str(row.get('experiment','')):<12} "
                f"{str(row.get('exp_name','')):<18} "
                f"{str(row.get('preprocess','')):<14} "
                f"{str(row.get('model','')):<6} "
                f"{row.get('horizon',''):>3} "
                f"{row.get('threshold',0):>6.3f}")
        for m in METRIC_NAMES:
            val = row.get(m)
            line += f"{val:>10.4f}" if val is not None else f"{'N/A':>10}"
        print(line)
    print()


def print_experiment_summary(experiments: list[dict]) -> None:
    """등록된 실험 목록을 요약 출력한다."""
    print(f"\n{'='*80}")
    print(f"  Registered Experiments ({len(experiments)}개)")
    print(f"{'='*80}")

    for exp in experiments:
        meta = exp["meta"]
        preprocess = meta.get("preprocessing", {})
        best = meta.get("best_model", {})
        n_results = len(exp["model_results"])

        print(f"\n  [{meta.get('experiment_id', '?')}] {meta.get('name', '')}")
        print(f"    Date:       {meta.get('date', '?')}")
        print(f"    Preprocess: {_preprocess_tag(preprocess)}")
        print(f"    Horizons:   {meta.get('horizons', [])}")
        print(f"    Models:     {meta.get('models', [])}  ({n_results} results)")
        if best:
            print(f"    Best:       {best.get('name')} H{best.get('horizon')} "
                  f"PR-AUC={best.get('test_pr_auc', '?')}")
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="실험 간 비교")
    parser.add_argument("--split", default="test", choices=["valid", "test"])
    parser.add_argument("--horizon", type=int, default=None)
    parser.add_argument("--metric", default="pr_auc", choices=METRIC_NAMES)
    parser.add_argument("--best-only", action="store_true",
                        help="실험별 best 모델만 출력")
    args = parser.parse_args()

    experiments = scan_experiments()

    if not experiments:
        print("results/exp_* 디렉터리가 없습니다.")
        return

    print_experiment_summary(experiments)

    if args.best_only:
        df = best_per_experiment(
            experiments, split=args.split,
            horizon=args.horizon, metric=args.metric,
        )
        title = f"Best Model per Experiment ({args.split} set, by {args.metric})"
        if args.horizon:
            title += f" — H{args.horizon}"
        print_table(df, title)
    else:
        df = build_cross_experiment_table(
            experiments, split=args.split, horizon=args.horizon,
        )
        title = f"All Results ({args.split} set)"
        if args.horizon:
            title += f" — H{args.horizon}"
        print_table(df, title)


if __name__ == "__main__":
    main()
