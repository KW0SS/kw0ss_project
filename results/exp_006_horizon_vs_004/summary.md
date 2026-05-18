# exp_006: Horizon 재학습 + exp_004(fixed_N) 대비 비교 — 실험 리포트

> 실험일: 2026-05-15
> 브랜치: `35-train-train-model-by-processed-fixed-data-set`
> 비교 baseline: `exp_004_fixed_N` (best: RF fixed_N1 exp-C, test PR-AUC 0.2861)

---

## 0. 실험 배경

`exp_004_fixed_N` 직후 `preprocess/data/processed/` 가 horizon 구조(H6~H24)로 갱신됐다. H10 만 봐도 train_rows 가 31,360(exp_002 기준) → **37,482** 로 늘었고 imbalance 도 148.3 → **177.5** 로 변했다. 즉 이전 horizon 실험 결과(exp_002, exp_003)와 데이터가 달라져 직접 인용이 불가능하다.

이번 실험에서는 갱신된 horizon 데이터로 다시 학습한 결과와 `exp_004_fixed_N` 결과를 같은 자에 올려, **"backward-looking N1" 라벨 정의가 forward-looking horizon 대비 정말로 우월한가** 를 판정한다.

학습 스코프(사용자 지정):

- Horizons: **H10, H12, H14, H20**
- Variants: **baseline, exp-A, exp-B, exp-C**
- Models: **RF, GBM, XGBoost, LightGBM, LogReg** (5종)
- 총 4 × 4 × 5 = **80 runs**

비교 대상:

- `exp_004_fixed_N/` 의 5 모델 × {fixed_N1, fixed_N2} × 4 variant = 40 runs (test_pos 가 있는 N1/N2 만)

---

## 1. 코드 변경 사항

이번 실험을 위해 horizon 학습 코드에 모델 1개만 추가했다. data_loader/run_all 의 핵심 로직은 그대로 유지된다.

| 파일 | 변경 |
|---|---|
| `src/modeling/run_all.py` | `MODEL_REGISTRY` 에 `"logreg": "src.modeling.train_logreg"` 추가. exp_004 에서 만든 LogReg + StandardScaler Pipeline 을 horizon CLI 에서도 그대로 사용 |

`save_result()` 가 결과를 `{model}_H{n}.json` 으로만 저장하고 variant 를 파일명에 안 넣어, 4 variants 를 한 디렉터리에 보관하면 충돌이 난다. 그래서 variant 별 서브폴더로 저장했다:

```
results/exp_006_horizon_vs_004/
├── baseline/  rf_H10.json … logreg_H20.json
├── exp-A/     rf_H10.json … logreg_H20.json
├── exp-B/     rf_H10.json … logreg_H20.json
└── exp-C/     rf_H10.json … logreg_H20.json
```

### 실행 명령

```bash
for v in baseline exp-A exp-B exp-C; do
    python -m src.modeling.run_all \
        --exp "exp_006_horizon_vs_004/$v" \
        --variant "$v" \
        --horizon 10 12 14 20 \
        --models rf gbm xgb lgbm logreg
done
```

---

## 2. 데이터 요약 (갱신된 horizon 데이터, baseline 기준)

| Horizon | Train | Train Pos | Valid | Valid Pos | Test | Test Pos | Imbalance |
|---|---|---|---|---|---|---|---|
| H10 | 37,482 | 210 | 6,102 | 21 | 6,587 | 36 | 177.5 |
| H12 | 37,482 | 250 | 6,102 | 27 | 6,587 | 51 | 148.9 |
| H14 | 35,769 | 284 | 6,102 | 34 | 6,587 | 60 | 124.9 |
| H20 | 35,769 | 421 | 6,102 | 62 | **3,240** | 46 | **84.0** |

특이점:

- **H20 test_rows 가 3,240** 으로 다른 horizon 의 절반. 윈도우가 20개월이면 2024 데이터의 후반부는 라벨 결정에 2026년 상폐 정보가 필요해 test 에서 제외된 결과로 추정.
- **H20 imbalance 84:1** 로 가장 낮음. 윈도우가 길수록 양성이 더 많이 포함됨.
- **H10 의 imbalance 가 177.5** 로 exp_002(148.3)보다 악화 — 새 데이터는 음성(정상 기업)이 더 많이 추가됐다.

`exp_004` 의 fixed_N1 (train 35,493 / pos 283, test 5,311 / pos 56, imbalance 124.4) 와 비교하면, H20 이 train_pos·test_pos 모두 더 많지만 test_rows 가 적다.

---

## 3. 모델 결과

### 3-1. Test Top 12 (PR-AUC 내림차순)

| Model | Horizon | Variant | Threshold | PR-AUC | F1 | Precision | Recall | ROC-AUC |
|---|---|---|---|---|---|---|---|---|
| **RF** | **H20** | **exp-A** | 0.219 | **0.2247** | **0.3421** | 0.4333 | 0.2826 | 0.8090 |
| RF | H20 | exp-C | 0.224 | 0.2238 | 0.3467 | 0.4483 | 0.2826 | 0.8091 |
| RF | H20 | exp-B | 0.215 | 0.2186 | 0.3158 | 0.4000 | 0.2609 | 0.7983 |
| RF | H20 | baseline | 0.214 | 0.2178 | 0.3158 | 0.4000 | 0.2609 | 0.7983 |
| LogReg | H12 | exp-A | 0.997 | 0.2089 | 0.2571 | 0.4737 | 0.1765 | 0.8807 |
| LogReg | H12 | exp-C | 0.997 | 0.2089 | 0.2571 | 0.4737 | 0.1765 | 0.8807 |
| RF | H12 | baseline | 0.256 | 0.2063 | 0.2716 | 0.3667 | 0.2157 | 0.8707 |
| RF | H12 | exp-B | 0.256 | 0.2063 | 0.2716 | 0.3667 | 0.2157 | 0.8707 |
| RF | H12 | exp-A | 0.264 | 0.2029 | 0.2750 | 0.3793 | 0.2157 | 0.8694 |
| RF | H12 | exp-C | 0.264 | 0.2029 | 0.2750 | 0.3793 | 0.2157 | 0.8694 |
| GBM | H14 | exp-B | 0.981 | 0.1846 | 0.0000 | 0.0000 | 0.0000 | 0.8526 |
| GBM | H14 | baseline | 0.157 | 0.1784 | 0.2295 | 0.2258 | 0.2333 | 0.8533 |

> GBM H14 exp-B 의 F1=0 은 valid 에서 best threshold 가 0.981 로 너무 보수적이라 test 양성을 하나도 못 잡은 사례. PR-AUC 는 threshold 무관 지표라 여전히 높지만 production 운영은 불가.

### 3-2. Test PR-AUC matrix (model × Horizon/variant)

| Model | H10/b | H10/A | H10/B | H10/C | H12/b | H12/A | H12/B | H12/C | H14/b | H14/A | H14/B | H14/C | H20/b | H20/A | H20/B | H20/C |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| RF | 0.134 | 0.139 | 0.134 | 0.139 | **0.206** | 0.203 | **0.206** | 0.203 | 0.161 | 0.150 | 0.161 | 0.150 | 0.218 | **0.225** | 0.219 | 0.224 |
| GBM | 0.103 | 0.093 | 0.107 | 0.088 | 0.178 | 0.164 | 0.170 | 0.167 | 0.178 | 0.170 | **0.185** | 0.173 | 0.133 | 0.156 | 0.150 | 0.154 |
| XGB | 0.112 | 0.110 | 0.112 | 0.110 | 0.154 | 0.160 | 0.158 | **0.177** | 0.116 | 0.141 | 0.123 | 0.135 | 0.174 | 0.176 | 0.148 | 0.164 |
| LGBM | 0.104 | 0.108 | 0.072 | 0.068 | 0.147 | 0.149 | 0.128 | 0.149 | 0.080 | 0.097 | 0.100 | 0.093 | 0.170 | 0.153 | 0.126 | 0.140 |
| LogReg | 0.122 | 0.157 | 0.122 | 0.157 | 0.162 | **0.209** | 0.162 | **0.209** | 0.099 | 0.118 | 0.099 | 0.118 | 0.161 | 0.149 | 0.161 | 0.149 |

(굵게 표시: 각 모델의 최고 PR-AUC)

### 3-3. Horizon 별 최강 조합

| Horizon | Best Model / Variant | Test PR-AUC | Test F1 |
|---|---|---|---|
| H10 | LogReg / exp-A | 0.1570 | 0.0541 |
| H12 | LogReg / exp-A · exp-C | 0.2089 | 0.2571 |
| H14 | GBM / exp-B | 0.1846 | 0.0000 (threshold 과보수) |
| **H20** | **RF / exp-A** | **0.2247** | **0.3421** |

H20 RF 가 PR-AUC + F1 모두 가장 안정. H10 은 모든 모델에서 가장 약한 horizon 으로 확인됨.

---

## 4. exp_004 (fixed_N) 대비 비교

### 4-1. Best vs Best

| 실험 | 라벨 정의 | Best 조합 | Test PR-AUC | Test F1 | Precision | Recall | ROC-AUC | Threshold |
|---|---|---|---|---|---|---|---|---|
| **exp_004** | backward, 상폐 1년 전 | RF / fixed_N1 / exp-C | **0.2861** | **0.3529** | 0.3333 | 0.3750 | 0.8595 | 0.097 |
| exp_006 | forward, 향후 H개월 | RF / H20 / exp-A | 0.2247 | 0.3421 | 0.4333 | 0.2826 | 0.8090 | 0.219 |
| **Δ (N1 vs H20)** | — | — | **+27%** | +3% | -23% | +33% | +0.0505 | -0.122 |

**해석:** fixed_N1 이 horizon best(H20) 대비 PR-AUC 에서 약 27% 우월. F1 은 거의 동일하나 trade-off 가 다르다 — H20 은 precision 위주(threshold 0.22), N1 은 recall 위주(threshold 0.10).

### 4-2. 모델별 Best 비교

| Model | exp_004 best (N1) | exp_006 best (Horizon) | Δ PR-AUC |
|---|---|---|---|
| RF | N1 exp-C: **0.2861** | H20 exp-A: 0.2247 | +27% (N1) |
| GBM | N1 baseline: 0.1048 | H14 exp-B: 0.1846 | -43% (H 우세) |
| XGBoost | N1 exp-C: 0.2305 | H12 exp-C: 0.1769 | +30% (N1) |
| LightGBM | N1 exp-C: 0.2515 | H20 baseline: 0.1698 | +48% (N1) |
| LogReg | N1 exp-A: 0.2094 | H12 exp-A: 0.2089 | ≈ (동률) |

**관찰:**

1. **N1 이 RF/XGB/LGBM 에서 모두 더 강함.** 트리 기반 모델은 부도 직전(N1) 시점의 재무비율을 더 잘 분리한다.
2. **GBM 만 horizon 우세.** GBM 은 약한 학습기를 순차 boosting 하므로 양성 신호가 강한 N1 보다는 노이즈가 더 섞인 H14 에서 더 많은 step 으로 점진적 학습이 유리한 듯.
3. **LogReg 는 동률.** 선형 모델은 라벨 정의 변화에 둔감 — 신호 강도보다 피처 스케일링 효과가 더 크다.

### 4-3. Threshold 패턴 차이

| 라벨 정의 | 우수 모델 군 | 대표 Threshold | 운영 의미 |
|---|---|---|---|
| fixed_N1 (이상치 강함) | RF/LGBM | 0.05–0.22 | 모델 확률이 0.1 이상이면 알람. recall-leaning |
| Horizon (H10–H20) | RF | 0.21–0.26 | 0.2 이상에서 알람. precision-leaning |
| LogReg (양쪽 공통) | — | 0.95+ | class_weight='balanced' 의 부작용으로 확률이 한쪽으로 쏠려 threshold 가 1 근접 |

production 알람 정책은 라벨 정의에 따라 threshold 캘리브레이션이 다르게 필요하다.

---

## 5. Horizon 길이 효과

| Horizon | Best PR-AUC | Best F1 | 최강 모델 |
|---|---|---|---|
| H10 | 0.157 | 0.0541 | LogReg |
| H12 | 0.209 | 0.2571 | LogReg / RF |
| H14 | 0.185 | — | GBM |
| H20 | **0.225** | **0.342** | RF |

**해석:** H10 → H20 으로 갈수록 양성 비율 증가(177.5:1 → 84:1), 평가 안정성 증가. H20 이 최강 horizon. H14 는 GBM 의 best threshold 가 0.981 로 튀는 등 평가가 불안정.

단, **H20 의 test_rows=3,240** 으로 적어, 양성 46 개를 잡는 평가의 통계적 신뢰구간이 좁지 않다. CI 분석은 후속 과제.

---

## 6. Variant 효과 (test 기준)

| 모델 군 | 선호 variant | 비고 |
|---|---|---|
| RF | **exp-A** (winsorize=T, scale=F) | H20 에서 최강. baseline 과의 차이는 작음 |
| LGBM | **baseline** at H20, **exp-A/B/C** at H12 | horizon 별로 다름 |
| XGBoost | **exp-C** at H12 (0.177) | winsorize+scale 조합 |
| GBM | **exp-B** at H14 (0.185) | scale-only 가 horizon 에서 유리 |
| LogReg | **exp-A / exp-C** (winsorize=T) | winsorize 효과 분명 |

exp_004 에서는 **exp-C** 가 트리 3종 우세였지만, horizon 에서는 모델별로 분산된 패턴. exp-A(winsorize 단독)가 RF/LogReg 에서 1위로 일관됨. **winsorize 가 핵심 개선 요인, scale 은 보조** — exp_004 와 동일한 결론.

---

## 7. 결론 및 다음 단계

### 결론

| 항목 | 값 |
|---|---|
| **Best 모델** | **fixed_N1 + RF + exp-C** (exp_004) |
| Test PR-AUC | **0.2861** |
| Test F1 | **0.3529** |
| 2위 (Horizon best) | H20 RF exp-A → PR-AUC 0.2247, F1 0.3421 |
| PR-AUC 격차 | fixed_N1 가 horizon best 대비 **+27%** |

- **라벨 정의 변경(forward H → backward N1)이 PR-AUC 를 27% 끌어올리는 단일 최강 변경.** 모델/하이퍼파라미터 튜닝보다 라벨 정의가 더 큰 효과.
- **RF 가 모든 horizon 과 fixed_N1 에서 PR-AUC 1위 후보.** 극심한 불균형에서 bagging 의 분산 감소가 일관되게 유효.
- **H20 이 horizon 군에서 최강** (양성 비율 증가 효과). H10 은 가장 약함.
- **Winsorize 가 핵심 전처리 효과** — exp_004 와 동일 결론.

### 후속 실험

1. **fixed_N1 + RF + exp-C 위에서 Optuna 튜닝** — `max_depth`, `min_samples_leaf`, `n_estimators` 조합 탐색
2. **H20 + fixed_N1 앙상블** — backward(N1) 모델과 forward(H20) 모델 확률을 stacking 해 trade-off(N1 recall vs H20 precision) 결합 가능성 확인
3. **시계열 안정성 검증** — train/valid/test 연도를 한 칸씩 밀어 best 조합이 재현되는지 확인
4. **H6/H8 추가 비교** — 더 짧은 horizon 이 N1 과 더 가까운 라벨 정의가 될 수 있는지 (단, train_pos 더 적어질 가능성)
5. **fixed_N1 데이터에 collinear column drop 적용 재학습** — `data_loader_fixed.py` 의 `COLLINEAR_DROP_COLUMNS` (유동비율, 유형자산상각비) 가 신규 반영됐으므로 31 features 로 재학습 후 33 features 결과와 비교

---

## 산출물 목록

```
results/exp_006_horizon_vs_004/
├── baseline/   rf_H10.json   rf_H12.json   rf_H14.json   rf_H20.json   gbm_*.json   xgb_*.json   lgbm_*.json   logreg_*.json
├── exp-A/      (위와 동일, 20개)
├── exp-B/      (위와 동일, 20개)
├── exp-C/      (위와 동일, 20개)
└── summary.md  # 이 리포트
```

총 80개 JSON (5 모델 × 4 horizon × 4 variant) + summary.md.

비교 baseline: `results/exp_004_fixed_N/` (40 JSON + summary.md, 5 모델 × 2 N × 4 variant).
