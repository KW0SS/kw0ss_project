"""fixed_N 데이터셋 학습/평가 실행 스크립트.

H-horizon 용 run_all.py 와 분리되어 있으며, 결과는 N{n}_{variant} 키로 저장된다.
fixed_N{n}/{variant} × model 의 모든 조합을 학습하고 results/{exp_dir}/ 아래에
JSON 결과 + 비교 테이블을 출력한다.

사용법:
    python -m src.modeling.run_all_fixed --exp exp_004_fixed_N --n 1 2 --variant baseline exp-A exp-B exp-C
    python -m src.modeling.run_all_fixed --exp exp_004_fixed_N --n 1 --models rf logreg
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.modeling.data_loader_fixed import (
    AVAILABLE_N_YEARS,
    load_and_prepare_fixed,
    print_data_summary_fixed,
)
from src.modeling.evaluate import (
    METRIC_NAMES,
    evaluate_model,
    find_best_threshold,
)

MODEL_REGISTRY: dict[str, str] = {
    "rf": "src.modeling.train_rf",
    "gbm": "src.modeling.train_gbm",
    "xgb": "src.modeling.train_xgboost",
    "lgbm": "src.modeling.train_lightgbm",
    "logreg": "src.modeling.train_logreg",
}

ALL_VARIANTS = ["baseline", "exp-A", "exp-B", "exp-C"]


def _load_train_fn(module_path: str):
    return importlib.import_module(module_path).train


def _save_result_fixed(
    *,
    model_name: str,
    n_years: int,
    variant: str,
    valid_metrics: dict,
    test_metrics: dict | None,
    params: dict,
    threshold: float,
    test_valid: bool,
    results_dir: Path,
) -> Path:
    """fixed_N 실험 결과를 JSON 으로 저장한다.

    파일명: {model}_N{n}_{variant}.json
    test_valid=False 이면 test 지표가 신뢰 불가(test_pos=0 등)임을 표시.
    """
    results_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "model_name": model_name,
        "n_years": n_years,
        "variant": variant,
        "threshold": threshold,
        "valid": valid_metrics,
        "test": test_metrics if test_valid else None,
        "test_valid": test_valid,
        "params": params,
        "timestamp": datetime.now().isoformat(),
    }
    filename = f"{model_name}_N{n_years}_{variant}.json"
    filepath = results_dir / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return filepath


def run_single(
    *,
    model_key: str,
    n_years: int,
    variant: str,
    data: dict,
    optimize_threshold: bool,
    results_dir: Path,
) -> dict:
    train_fn = _load_train_fn(MODEL_REGISTRY[model_key])

    print(f"--- Training {model_key} on fixed_N{n_years} / {variant} ---")
    model, params = train_fn(
        data["X_train"], data["y_train"],
        data["X_valid"], data["y_valid"],
    )

    valid_result = evaluate_model(model, data["X_valid"], data["y_valid"])
    threshold = 0.5
    if optimize_threshold and int(data["y_valid"].sum()) > 0:
        threshold, best_f1 = find_best_threshold(
            data["y_valid"], valid_result["y_proba"], metric="f1",
        )
        print(f"  Best threshold (F1): {threshold:.4f} (F1={best_f1:.4f})")
        valid_result = evaluate_model(model, data["X_valid"], data["y_valid"], threshold=threshold)
    valid_metrics = valid_result["metrics"]

    test_pos = int(data["y_test"].sum())
    test_valid = test_pos > 0
    test_metrics: dict | None = None
    if test_valid:
        test_result = evaluate_model(
            model, data["X_test"], data["y_test"], threshold=threshold,
        )
        test_metrics = test_result["metrics"]
        print(f"  Valid: F1={valid_metrics['f1']:.4f}  PR-AUC={valid_metrics['pr_auc']:.4f}  "
              f"ROC-AUC={valid_metrics['roc_auc']:.4f}")
        print(f"  Test:  F1={test_metrics['f1']:.4f}  PR-AUC={test_metrics['pr_auc']:.4f}  "
              f"ROC-AUC={test_metrics['roc_auc']:.4f}")
    else:
        print(f"  Valid: F1={valid_metrics['f1']:.4f}  PR-AUC={valid_metrics['pr_auc']:.4f}  "
              f"ROC-AUC={valid_metrics['roc_auc']:.4f}")
        print(f"  Test:  skipped (test positive = 0)")

    filepath = _save_result_fixed(
        model_name=model_key,
        n_years=n_years,
        variant=variant,
        valid_metrics=valid_metrics,
        test_metrics=test_metrics,
        params=params,
        threshold=threshold,
        test_valid=test_valid,
        results_dir=results_dir,
    )
    print(f"  Saved: {filepath.relative_to(PROJECT_ROOT)}")
    print()

    return {
        "model": model_key,
        "n_years": n_years,
        "variant": variant,
        "threshold": threshold,
        "valid": valid_metrics,
        "test": test_metrics,
        "test_valid": test_valid,
    }


def print_comparison(results: list[dict], split: str = "test") -> None:
    rows = [r for r in results if split != "test" or r["test_valid"]]
    if not rows:
        print(f"비교 가능한 {split} 결과 없음")
        return
    rows = sorted(
        rows,
        key=lambda r: (r[split] or {}).get("pr_auc", 0.0),
        reverse=True,
    )
    print(f"\n=== Model Comparison ({split} set, fixed_N) ===")
    header = f"{'Model':<10} {'N':>2} {'Variant':<10} {'Thr':>6}"
    header += "".join(f"{m:>11}" for m in METRIC_NAMES)
    print(header)
    print("-" * len(header))
    for r in rows:
        metrics = r[split] or {}
        line = f"{r['model']:<10} {r['n_years']:>2} {r['variant']:<10} {r['threshold']:>6.3f}"
        for m in METRIC_NAMES:
            val = metrics.get(m)
            line += f"{val:>11.4f}" if val is not None else f"{'N/A':>11}"
        print(line)
    print()


def run_experiments(
    *,
    n_years_list: list[int],
    variants: list[str],
    model_keys: list[str],
    optimize_threshold: bool,
    include_macro: bool,
    include_raw_value: bool,
    exp_dir: str,
) -> list[dict]:
    results_dir = PROJECT_ROOT / "results" / exp_dir
    results_dir.mkdir(parents=True, exist_ok=True)
    print(f"Results dir: {results_dir.relative_to(PROJECT_ROOT)}")

    all_results: list[dict] = []

    for n_years in n_years_list:
        for variant in variants:
            print(f"\n{'='*64}")
            print(f"  fixed_N{n_years} / {variant}")
            print(f"{'='*64}\n")

            try:
                data = load_and_prepare_fixed(
                    n_years,
                    include_macro=include_macro,
                    include_raw_value=include_raw_value,
                    variant=variant,
                )
            except FileNotFoundError as e:
                print(f"  [SKIP] {e}")
                continue
            print_data_summary_fixed(n_years, variant, data)

            for key in model_keys:
                if key not in MODEL_REGISTRY:
                    print(f"  [SKIP] Unknown model: {key}")
                    continue
                try:
                    result = run_single(
                        model_key=key,
                        n_years=n_years,
                        variant=variant,
                        data=data,
                        optimize_threshold=optimize_threshold,
                        results_dir=results_dir,
                    )
                    all_results.append(result)
                except ImportError as e:
                    print(f"  [SKIP] {key}: {e}")
                except Exception as e:
                    print(f"  [ERROR] {key}: {e}")

    if all_results:
        print_comparison(all_results, split="valid")
        print_comparison(all_results, split="test")

    return all_results


def main() -> None:
    parser = argparse.ArgumentParser(description="fixed_N 데이터셋 모델 실험")
    parser.add_argument("--exp", type=str, required=True, help="결과 디렉터리 이름 (results/{exp})")
    parser.add_argument(
        "--n", nargs="+", default=["1", "2"],
        help=f"학습할 N 값 (사용 가능: {AVAILABLE_N_YEARS}) 또는 'all'",
    )
    parser.add_argument(
        "--variant", nargs="+", default=ALL_VARIANTS,
        help=f"전처리 variant (선택지: {ALL_VARIANTS})",
    )
    parser.add_argument(
        "--models", nargs="+", default=list(MODEL_REGISTRY.keys()),
        help=f"모델 키 (선택지: {list(MODEL_REGISTRY.keys())})",
    )
    parser.add_argument("--no-threshold-opt", action="store_true")
    parser.add_argument("--no-macro", action="store_true")
    parser.add_argument("--no-raw-value", action="store_true")

    args = parser.parse_args()

    if "all" in args.n:
        n_years_list = AVAILABLE_N_YEARS
    else:
        n_years_list = [int(x) for x in args.n]

    run_experiments(
        n_years_list=n_years_list,
        variants=args.variant,
        model_keys=args.models,
        optimize_threshold=not args.no_threshold_opt,
        include_macro=not args.no_macro,
        include_raw_value=not args.no_raw_value,
        exp_dir=args.exp,
    )


if __name__ == "__main__":
    main()
