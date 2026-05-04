# exp_002: processed_v3 baseline — 실험 리포트

> 실험일: 2026-05-04
> 브랜치: `24-eda-re-run-exploratory-data-analysis-on-the-cleaned-dataset`

---

## 0. 실험 배경

EDA 재실행을 통해 정제된 새 데이터셋 `preprocess/data/processed_v3/`(현재 `processed/`로 이동)을 검증하기 위한 sanity-check 실험.
exp_001 (`exp_001_baseline_clip`)과 동일 모델 구성으로 H10 baseline 변형만 학습하여, 신규 데이터로 성능이 유지/개선되는지 확인하는 것이 목적.

### 데이터 변경 사항

| 항목 | exp_001 (구버전) | exp_002 (v3) |
|---|---|---|
| 데이터 경로 | `processed/H{n}/{train,valid,test}.csv` | `processed/H{n}/{variant}/{train,valid,test}.csv` |
| Variant 구조 | 단일 (clipping만) | baseline / exp-A / exp-B / exp-C 4종 |
| H10 train rows | 30,764 | 31,360 (+1.9%) |
| H10 train positive | 193 | 210 (+8.8%) |
| H10 test positive | 29 | 36 (+24%) |
| H10 imbalance ratio | 158.4:1 | 148.3:1 |

### Variant 정의 (meta.json 기준)

| Variant | winsorize | robust_scale |
|---|---|---|
| **baseline** (이번 실험) | False | False |
| exp-A | True | False |
| exp-B | False | True |
| exp-C | True | True |

이번 실험은 baseline만 검증, 나머지 3개 변형은 후속 실험 대상.

---

## 1. 코드 변경 사항

신규 디렉터리 구조 지원을 위해 로더/CLI 확장.

| 파일 | 변경 |
|---|---|
| `src/modeling/data_loader.py` | `variant` 인자 추가 (기본 `"baseline"`). `_resolve_horizon_dir()`로 `H{n}/{variant}/` 우선 탐색, 없으면 `H{n}/` 폴백 (구조 호환) |
| `src/modeling/run_all.py` | `--variant` CLI 옵션 추가 (`baseline`/`exp-A`/`exp-B`/`exp-C`) |

실행 명령:

```bash
python3 -m src.modeling.run_all --exp exp_002_v3_baseline --variant baseline --horizon 10
```

---

## 2. 데이터 요약 (H10 baseline)

| 항목 | 값 |
|---|---|
| Features | 33개 (재무비율 21 + 원시값 5 + 매크로 6 + 유보 1) |
| Train | 31,360행 (positive 210) |
| Valid | 5,050행 (positive 21) |
| Test | 5,311행 (positive 36) |
| Imbalance ratio | 148.3:1 |
| Train years | 2015–2022 |
| Valid year | 2023 |
| Test year | 2024 |
| Impute 후 잔존 NaN | 0 |

---

## 3. 모델 결과

### 3-1. Test set (PR-AUC 내림차순)

| Model | PR-AUC | F1 | Precision | Recall | ROC-AUC | Threshold |
|---|---|---|---|---|---|---|
| **RF** | **0.1735** | **0.3158** | **0.4286** | **0.2500** | 0.8549 | 0.283 |
| XGBoost | 0.1356 | 0.0476 | 0.1667 | 0.0278 | 0.8661 | 0.355 |
| LightGBM | 0.1107 | 0.0000 | 0.0000 | 0.0000 | 0.8752 | 0.797 |
| GBM | 0.1063 | 0.1695 | 0.2174 | 0.1389 | 0.8681 | 0.251 |

### 3-2. Valid set (모델 선정 기준)

| Model | PR-AUC | F1 | Precision | Recall | ROC-AUC | Threshold |
|---|---|---|---|---|---|---|
| LightGBM | 0.1950 | 0.2500 | 1.0000 | 0.1429 | 0.9250 | 0.797 |
| XGBoost | 0.1874 | 0.2857 | 0.5714 | 0.1905 | 0.9314 | 0.355 |
| **RF** | **0.1811** | **0.3000** | 0.3158 | 0.2857 | 0.9112 | 0.283 |
| GBM | 0.1174 | 0.2222 | 0.2083 | 0.2381 | 0.8920 | 0.251 |

---

## 4. exp_001 대비 비교 (H10 RF)

| Metric | exp_001 (구) | exp_002 (v3) | Δ |
|---|---|---|---|
| Test PR-AUC | 0.1068 | **0.1735** | **+62%** |
| Test F1 | 0.1667 | **0.3158** | **+89%** |
| Test Precision | 0.1613 | **0.4286** | **+166%** |
| Test Recall | 0.1724 | 0.2500 | +45% |
| Test ROC-AUC | 0.8367 | **0.8549** | +0.018 |
| Threshold | 0.230 | 0.283 | +0.053 |

**개선 원인 추정:**

1. **Positive 샘플 증가** — train 193 → 210 (+9%), test 29 → 36 (+24%). 학습 신호 강화 + 평가 안정성 향상.
2. **EDA 재정제 효과** — v3는 정제된 데이터셋으로, 노이즈/이상치가 줄어들어 모델이 진짜 시그널을 학습할 여지가 늘어남 (정확한 변경 내용은 EDA PR `#24` 참조).
3. **클래스 분포 균형 개선** — imbalance ratio 158.4 → 148.3로 약간 완화.

---

## 5. 분석

**RF 우위 패턴 재현:**
- exp_001과 동일하게 RF가 PR-AUC/F1에서 1위. 부스팅 모델은 valid에서 LightGBM이 PR-AUC 1위였으나 test에서 F1=0 — threshold 0.797로 과보수적이라 generalization 실패.
- 극심한 불균형 환경에서는 bagging의 분산 감소가 여전히 유리하다는 exp_001의 결론이 v3 데이터에서도 유지됨.

**LightGBM의 test F1=0 문제:**
- valid에서 PR-AUC=0.195로 가장 높았으나 test에서 모든 예측이 negative.
- valid threshold 0.797이 valid set의 21개 positive 중 단 1개만 잡는 극단적으로 보수적인 지점 (precision=1.0, recall=0.143).
- 이런 threshold는 valid positive 수가 극소수일 때 발생하는 과적합 현상으로, 후속 실험에서는 threshold 안정화 전략 필요.

**XGBoost의 test 성능 저하:**
- valid F1=0.286 → test F1=0.048로 generalization gap 큼.
- early stopping이 best_iteration=78에서 중단됐으나 valid set positive 21개로는 PR-AUC 신호가 불안정.

---

## 6. 결론 및 다음 단계

### 결론

| 항목 | 값 |
|---|---|
| Best 모델 | **Random Forest** |
| Variant | baseline (winsorize/robust_scale 모두 False) |
| Test PR-AUC | **0.1735** |
| Test F1 | **0.3158** |
| exp_001 대비 | PR-AUC +62%, F1 +89% |

v3 데이터셋이 모델 성능을 명확히 개선함. RF baseline 전략 (Phase 2 결론)이 새 데이터셋에서도 유효함을 확인.

### 다음 단계

1. **변형 비교 (exp-A/B/C)** — winsorize, robust_scale 효과 측정
   ```bash
   python3 -m src.modeling.run_all --exp exp_003_v3_winsor --variant exp-A --horizon 10
   python3 -m src.modeling.run_all --exp exp_004_v3_robust --variant exp-B --horizon 10
   python3 -m src.modeling.run_all --exp exp_005_v3_winsor_robust --variant exp-C --horizon 10
   ```
2. **전 horizon 확장** — `--horizon all` 로 H6~H24 sweep, sweet spot 재탐색
3. **하이퍼파라미터 튜닝** — RF/XGB/LGBM Optuna 적용
4. **Threshold 안정화** — valid positive 수가 적은 환경에서의 안정 threshold 전략 (예: bootstrap CI, 다중 horizon 평균)

---

## 산출물 목록

```
results/exp_002_v3_baseline/
├── rf_H10.json         # RF 결과 (best)
├── gbm_H10.json        # GBM 결과
├── xgb_H10.json        # XGBoost 결과
├── lgbm_H10.json       # LightGBM 결과
└── summary.md          # 이 리포트
```
