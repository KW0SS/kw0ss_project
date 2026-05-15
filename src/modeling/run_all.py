"""
전체 모델 실험 실행 스크립트.

H-horizon별로 등록된 모델을 순차 학습/평가하고
결과를 results/exp_XXX_<name>/ 디렉터리에 저장한다.

사용법:
    python -m src.modeling.run_all --exp exp_001_baseline_clip
    python -m src.modeling.run_all --exp exp_002_winsor --horizon 10 12
    python -m src.modeling.run_all --exp exp_001_baseline_clip --models rf xgb
    python -m src.modeling.run_all --exp exp_001_baseline_clip --horizon all
"""

from __future__ import annotations

import argparse
import importlib
import sys
from pathlib import Path

# 프로젝트 루트를 path에 추가
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.modeling.data_loader import (
    AVAILABLE_HORIZONS,
    load_and_prepare,
    print_data_summary,
)
from src.modeling.evaluate import (
    evaluate_model,
    find_best_threshold,
    load_results,
    print_comparison,
    save_result,
)

# ---------------------------------------------------------------------------
# 모델 레지스트리
# ---------------------------------------------------------------------------

# 각 모델 모듈은 train(X_train, y_train, X_valid, y_valid) -> (model, params) 를 제공해야 함
MODEL_REGISTRY: dict[str, str] = {
    "rf": "src.modeling.train_rf",
    "gbm": "src.modeling.train_gbm",
    "xgb": "src.modeling.train_xgboost",
    "lgbm": "src.modeling.train_lightgbm",
    "catboost": "src.modeling.train_catboost",
    "logreg": "src.modeling.train_logreg",
}


def _load_train_fn(module_path: str):
    """모듈에서 train 함수를 동적으로 로드한다."""
    mod = importlib.import_module(module_path)
    return mod.train


# ---------------------------------------------------------------------------
# 단일 실험 실행
# ---------------------------------------------------------------------------


def run_single(
    model_key: str,
    horizon: int,
    data: dict,
    optimize_threshold: bool = True,
    results_dir: Path | None = None,
) -> dict:
    """단일 모델 × 단일 horizon 실험을 실행한다.

    Returns:
        {"model_name", "horizon", "valid": metrics, "test": metrics, ...}
    """
    module_path = MODEL_REGISTRY[model_key]
    train_fn = _load_train_fn(module_path)

    print(f"--- Training {model_key} on H{horizon} ---")
    model, params = train_fn(
        data["X_train"], data["y_train"],
        data["X_valid"], data["y_valid"],
    )

    # valid 평가
    valid_result = evaluate_model(model, data["X_valid"], data["y_valid"])
    valid_metrics = valid_result["metrics"]

    # threshold 최적화
    threshold = 0.5
    if optimize_threshold:
        threshold, best_f1 = find_best_threshold(
            data["y_valid"], valid_result["y_proba"], metric="f1",
        )
        print(f"  Best threshold (F1): {threshold:.4f} (F1={best_f1:.4f})")

        # 최적 threshold로 valid 재평가
        valid_result = evaluate_model(
            model, data["X_valid"], data["y_valid"], threshold=threshold,
        )
        valid_metrics = valid_result["metrics"]

    # test 평가 (최적 threshold 적용)
    test_result = evaluate_model(
        model, data["X_test"], data["y_test"], threshold=threshold,
    )
    test_metrics = test_result["metrics"]

    # 결과 출력
    print(f"  Valid: F1={valid_metrics['f1']:.4f}  PR-AUC={valid_metrics['pr_auc']:.4f}  "
          f"ROC-AUC={valid_metrics['roc_auc']:.4f}")
    print(f"  Test:  F1={test_metrics['f1']:.4f}  PR-AUC={test_metrics['pr_auc']:.4f}  "
          f"ROC-AUC={test_metrics['roc_auc']:.4f}")

    # 저장
    filepath = save_result(
        model_name=model_key,
        horizon=horizon,
        valid_metrics=valid_metrics,
        test_metrics=test_metrics,
        params=params,
        threshold=threshold,
        results_dir=results_dir,
    )
    print(f"  Saved: {filepath.relative_to(PROJECT_ROOT)}")
    print()

    return {
        "model_name": model_key,
        "horizon": horizon,
        "valid": valid_metrics,
        "test": test_metrics,
        "threshold": threshold,
        "params": params,
    }


# ---------------------------------------------------------------------------
# 전체 실행
# ---------------------------------------------------------------------------


def run_experiments(
    horizons: list[int],
    model_keys: list[str],
    optimize_threshold: bool = True,
    include_macro: bool = True,
    include_raw_value: bool = True,
    exp_dir: str | None = None,
    variant: str = "baseline",
) -> list[dict]:
    """복수 모델 × 복수 horizon 실험을 실행한다.

    Args:
        exp_dir: 실험 결과 저장 디렉터리 이름 (예: "exp_001_baseline_clip").
                 None이면 results/ 루트에 저장.
    """
    results_dir = None
    if exp_dir:
        results_dir = PROJECT_ROOT / "results" / exp_dir
        results_dir.mkdir(parents=True, exist_ok=True)
        print(f"Results dir: {results_dir.relative_to(PROJECT_ROOT)}")

    all_results = []

    for horizon in horizons:
        print(f"\n{'='*60}")
        print(f"  Horizon: H{horizon}")
        print(f"{'='*60}\n")

        data = load_and_prepare(
            horizon,
            include_macro=include_macro,
            include_raw_value=include_raw_value,
            variant=variant,
        )
        print_data_summary(horizon, data)

        for key in model_keys:
            if key not in MODEL_REGISTRY:
                print(f"  [SKIP] Unknown model: {key}")
                continue
            try:
                result = run_single(
                    key, horizon, data, optimize_threshold,
                    results_dir=results_dir,
                )
                all_results.append(result)
            except ImportError as e:
                print(f"  [SKIP] {key}: {e}")
            except Exception as e:
                print(f"  [ERROR] {key}: {e}")

    # 최종 비교
    if all_results:
        print_comparison(all_results, split="test")

    return all_results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Tree-based 모델 실험 실행")
    parser.add_argument(
        "--exp", type=str, default=None,
        help="실험 디렉터리 이름 (예: exp_002_winsor). 없으면 results/ 루트에 저장",
    )
    parser.add_argument(
        "--horizon", nargs="+", default=["10"],
        help="실험할 horizon (예: 10 12 또는 all)",
    )
    parser.add_argument(
        "--models", nargs="+", default=list(MODEL_REGISTRY.keys()),
        help=f"실험할 모델 키 (선택지: {list(MODEL_REGISTRY.keys())})",
    )
    parser.add_argument(
        "--no-threshold-opt", action="store_true",
        help="threshold 최적화 비활성화",
    )
    parser.add_argument("--no-macro", action="store_true", help="매크로 변수 제외")
    parser.add_argument("--no-raw-value", action="store_true", help="원시값 컬럼 제외")
    parser.add_argument(
        "--variant", type=str, default="baseline",
        choices=["baseline", "exp-A", "exp-B", "exp-C"],
        help="전처리 변형 선택 (processed/H{n}/{variant})",
    )

    args = parser.parse_args()

    # horizon 파싱
    if "all" in args.horizon:
        horizons = AVAILABLE_HORIZONS
    else:
        horizons = [int(h) for h in args.horizon]

    run_experiments(
        horizons=horizons,
        model_keys=args.models,
        optimize_threshold=not args.no_threshold_opt,
        include_macro=not args.no_macro,
        include_raw_value=not args.no_raw_value,
        exp_dir=args.exp,
        variant=args.variant,
    )


if __name__ == "__main__":
    main()
