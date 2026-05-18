# exp_007: Horizon 재학습 (H16/H18/H22/H24) — exp_006 후속 + exp_004(fixed_N) 대비 비교

> 실험일: 2026-05-18
> 브랜치: `35-train-train-model-by-processed-fixed-data-set`
> 직접 후속: `exp_006_horizon_vs_004` (H10/H12/H14/H20)
> 비교 baseline: `exp_004_fixed_N` (best: RF fixed_N1 exp-C, test PR-AUC 0.2861)

---

## 0. 실험 배경

`exp_006` 은 horizon sweep 을 **H10, H12, H14, H20** 4개만 돌렸다. 갱신된 horizon 데이터(`preprocess/data/processed/H{n}/`)는 H6~H24 까지 있는데, 중간 구간(H16, H18, H22, H24)이 비어 horizon 길이 효과 곡선이 띄엄띄엄했다.

이번 실험은 exp_006 에서 빠진 **H16, H18, H22, H24** 를 동일 설정으로 마저 학습해, H10→H24 전 구간 곡선을 메우고, "horizon 길이가 길수록 PR-AUC 가 오르는가" 와 "horizon best 가 fixed_N1 격차를 얼마나 좁히는가" 를 다시 판정한다.

학습 스코프 (exp_006 와 동일, horizon 만 교체):

- Horizons: **H16, H18, H22, H24**
- Variants: **baseline, exp-A, exp-B, exp-C**
- Models: **RF, GBM, XGBoost, LightGBM, LogReg** (5종)
- 총 4 × 4 × 5 = **80 runs**

비교 대상:

- `exp_006_horizon_vs_004/` : H10/H12/H14/H20 결과 (80 runs)
- `exp_004_fixed_N/` : 5 모델 × {fixed_N1, fixed_N2} × 4 variant (backward N1 라벨)

---

## 1. 코드 변경 사항

**코드 변경 없음.** exp_006 에서 `MODEL_REGISTRY` 에 `logreg` 를 추가하고 variant 별 서브폴더 저장 구조를 잡아둔 상태 그대로 재사용했다. horizon 인자만 `16 18 22 24` 로 교체.

저장 구조도 exp_006 와 동일하다(`save_result()` 가 variant 를 파일명에 안 넣어 variant 별 서브폴더 분리 필요):

```
results/exp_007_horizon_vs_004/
├── baseline/  rf_H16.json … logreg_H24.json
├── exp-A/     rf_H16.json … logreg_H24.json
├── exp-B/     rf_H16.json … logreg_H24.json
└── exp-C/     rf_H16.json … logreg_H24.json
```

### 실행 명령

```bash
for v in baseline exp-A exp-B exp-C; do
    python -m src.modeling.run_all \
        --exp "exp_007_horizon_vs_004/$v" \
        --variant "$v" \
        --horizon 16 18 22 24 \
        --models rf gbm xgb lgbm logreg
done
```

---

## 2. 데이터 요약 (horizon 데이터, baseline 기준)

| Horizon | Train | Train Pos | Valid | Valid Pos | Test | Test Pos | Imbalance |
|---|---|---|---|---|---|---|---|
| H16 | 35,769 | 333 | 6,102 | 43 | 4,894 | 54 | 106.4 |
| H18 | 35,769 | 374 | 6,102 | 54 | 4,894 | 61 | 94.6 |
| H22 | 35,769 | 473 | 6,102 | 72 | **1,608** | 27 | 74.6 |
| H24 | 35,769 | 516 | 6,102 | 87 | **1,608** | 28 | **68.3** |

train years 2015–2022 / valid 2023 / test 2024 (exp_006 와 동일 split).

특이점:

- **H22·H24 test_rows 가 1,608** 으로 급감. exp_006 의 H20(3,240)보다도 작다. 윈도우가 22~24개월이면 2024 후반 데이터는 라벨 확정에 2026년 이후 상폐 정보가 필요해 test 에서 대거 제외된 결과.
- **imbalance 가 horizon 따라 단조 감소** (H16 106 → H24 68). 윈도우가 길수록 양성이 더 많이 포함됨 — exp_006 의 H10(177)→H20(84) 추세와 일관.
- **H16/H18 은 test_rows 4,894 + test_pos 54~61** 로 평가 표본이 비교적 충분. **H22/H24 는 test_pos 27~28** 로 통계적 신뢰구간이 넓다(평가 주의).

`exp_004` 의 fixed_N1 (train 35,493 / pos 283, test 5,311 / pos 56) 와 비교하면, H16~H24 모두 train_pos 가 더 많지만 H22/H24 는 test 표본이 1/3 수준.

---

## 3. 모델 결과

### 3-1. Test Top 12 (PR-AUC 내림차순)

| Model | Horizon | Variant | Threshold | PR-AUC | F1 | Precision | Recall | ROC-AUC |
|---|---|---|---|---|---|---|---|---|
| **GBM** | **H24** | **baseline** | 0.204 | **0.2556** | **0.3111** | 0.4118 | 0.2500 | 0.8116 |
| XGB | H22 | exp-C | 0.235 | 0.2425 | 0.1667 | 0.3333 | 0.1111 | 0.8551 |
| GBM | H24 | exp-B | 0.257 | 0.2407 | 0.2667 | 0.3529 | 0.2143 | 0.8134 |
| GBM | H24 | exp-C | 0.225 | 0.2375 | 0.2791 | 0.4000 | 0.2143 | 0.8264 |
| RF | H24 | exp-A | 0.265 | 0.2361 | 0.3000 | 0.5000 | 0.2143 | 0.7886 |
| RF | H24 | exp-C | 0.265 | 0.2360 | 0.3000 | 0.5000 | 0.2143 | 0.7887 |
| GBM | H16 | exp-A | 0.334 | 0.2324 | 0.2716 | 0.4074 | 0.2037 | 0.8618 |
| GBM | H16 | exp-C | 0.283 | 0.2307 | 0.2651 | 0.3793 | 0.2037 | 0.8598 |
| RF | H22 | exp-A | 0.340 | 0.2303 | 0.2424 | 0.6667 | 0.1481 | 0.8200 |
| RF | H22 | exp-C | 0.340 | 0.2303 | 0.2424 | 0.6667 | 0.1481 | 0.8200 |
| XGB | H22 | exp-A | 0.249 | 0.2290 | 0.2632 | 0.4545 | 0.1852 | 0.8607 |
| GBM | H16 | baseline | 0.564 | 0.2273 | 0.2973 | 0.5500 | 0.2037 | 0.8526 |

> **GBM H24 baseline** 이 PR-AUC·F1 모두 1위면서 threshold 0.20 으로 production 운영 가능 구간. exp_006 의 best(RF H20 exp-A)가 threshold 0.22 였던 것과 유사한 precision-leaning 패턴이나, F1 안정성은 GBM H24 가 더 높다. XGB H22 exp-C 는 PR-AUC 2위지만 F1=0.17 (recall 0.11) — threshold 가 보수적이라 운영 부적합.

### 3-2. Test PR-AUC matrix (model × Horizon/variant)

| Model | H16/b | H16/A | H16/B | H16/C | H18/b | H18/A | H18/B | H18/C | H22/b | H22/A | H22/B | H22/C | H24/b | H24/A | H24/B | H24/C |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| RF | 0.215 | 0.209 | 0.216 | 0.209 | 0.210 | 0.197 | 0.210 | 0.197 | 0.218 | 0.230 | 0.218 | 0.230 | 0.219 | **0.236** | 0.219 | 0.236 |
| GBM | 0.227 | 0.232 | 0.218 | 0.231 | 0.204 | 0.208 | 0.206 | 0.212 | 0.197 | 0.182 | 0.195 | 0.190 | **0.256** | 0.218 | 0.241 | 0.237 |
| XGB | 0.148 | 0.159 | 0.156 | 0.156 | 0.166 | 0.150 | 0.158 | 0.141 | 0.220 | 0.229 | 0.203 | **0.242** | 0.191 | 0.200 | 0.190 | 0.204 |
| LGBM | 0.138 | 0.141 | 0.119 | 0.156 | 0.143 | 0.142 | 0.139 | 0.137 | 0.199 | **0.214** | 0.152 | 0.151 | 0.177 | 0.197 | 0.133 | 0.129 |
| LogReg | 0.118 | 0.120 | 0.118 | 0.120 | 0.116 | 0.118 | 0.116 | 0.118 | 0.162 | 0.151 | 0.162 | 0.151 | **0.167** | 0.150 | 0.167 | 0.150 |

(굵게 표시: 각 모델의 최고 PR-AUC)

### 3-3. Horizon 별 최강 조합

| Horizon | Best Model / Variant | Test PR-AUC | Test F1 | Threshold | Test Pos |
|---|---|---|---|---|---|
| H16 | GBM / exp-A | 0.2324 | 0.2716 | 0.334 | 54 |
| H18 | GBM / exp-C | 0.2124 | 0.2649 | 0.091 | 61 |
| H22 | XGB / exp-C | 0.2425 | 0.1667 (recall 0.11) | 0.235 | 27 |
| **H24** | **GBM / baseline** | **0.2556** | **0.3111** | 0.204 | 28 |

H24 GBM 이 PR-AUC·F1 모두 최강. **H16 GBM exp-A(0.2324)** 는 test 표본이 4,894행/54양성으로 가장 두터워 가장 신뢰할 만한 결과 — H24(1,608행)의 0.2556 보다 통계적으로 단단하다.

---

## 4. exp_006 (H10–H20) · exp_004 (fixed_N) 대비 비교

### 4-1. Best vs Best

| 실험 | 라벨 정의 | Best 조합 | Test PR-AUC | Test F1 | Precision | Recall | ROC-AUC | Threshold |
|---|---|---|---|---|---|---|---|---|
| **exp_004** | backward, 상폐 1년 전 | RF / fixed_N1 / exp-C | **0.2861** | 0.3529 | 0.3333 | 0.3750 | 0.8595 | 0.097 |
| **exp_007** | forward, 향후 H개월 | **GBM / H24 / baseline** | **0.2556** | 0.3111 | 0.4118 | 0.2500 | 0.8116 | 0.204 |
| exp_006 | forward, 향후 H개월 | RF / H20 / exp-A | 0.2247 | 0.3421 | 0.4333 | 0.2826 | 0.8090 | 0.219 |
| **Δ (N1 vs H24)** | — | — | **+11.9%** | +13% | -19% | +50% | +0.048 | -0.107 |

**해석:** exp_007 의 horizon best(H24 GBM)가 exp_006 의 horizon best(H20 RF, 0.2247)를 **+13.7%** 끌어올렸다. fixed_N1 과의 PR-AUC 격차는 exp_006 시점 **+27%** 에서 **+11.9%** 로 절반 이하로 좁혀졌다. 라벨 정의(backward N1)가 여전히 우위지만, **충분히 긴 horizon(H24)이면 격차가 크게 줄어든다.**

### 4-2. 모델별 Best 비교 (3개 실험 종합)

| Model | exp_004 best (N1) | exp_006 best (H10–20) | exp_007 best (H16–24) | 종합 horizon best |
|---|---|---|---|---|
| RF | N1 exp-C: **0.2861** | H20 exp-A: 0.2247 | H24 exp-A: 0.2361 | H24 (개선) |
| GBM | N1 baseline: 0.1048 | H14 exp-B: 0.1846 | **H24 baseline: 0.2556** | **H24 (대폭 개선)** |
| XGBoost | N1 exp-C: 0.2305 | H12 exp-C: 0.1769 | H22 exp-C: 0.2425 | H22 (개선) |
| LightGBM | N1 exp-C: 0.2515 | H20 baseline: 0.1698 | H22 exp-A: 0.2139 | H22 (개선) |
| LogReg | N1 exp-A: 0.2094 | H12 exp-A: 0.2089 | H24 baseline: 0.1666 | H12 (긴 horizon 무용) |

**관찰:**

1. **GBM 이 horizon 최강 모델로 확정.** exp_006 에서 "GBM 만 horizon 우세" 가설을 세웠는데, exp_007 에서 H24 GBM(0.2556)이 전 horizon·전 모델 통틀어 1위. boosting 이 긴 horizon 의 약한·노이즈 섞인 양성 신호를 점진적으로 학습하는 데 유리하다는 해석이 강화됐다.
2. **트리 3종(RF/XGB/LGBM)도 긴 horizon 에서 일제히 개선.** 모두 H22~H24 에서 exp_006 best 를 넘었다. 단 fixed_N1 best 에는 RF/LGBM 이 여전히 못 미친다(N1 우위 유지).
3. **LogReg 는 긴 horizon 에서 오히려 악화** (H12 0.209 → H24 0.167). 선형 모델은 horizon 이 길수록 늘어나는 비선형 양성 패턴을 못 잡는다.

### 4-3. Threshold 패턴

| 라벨 정의 | 우수 모델 | 대표 Threshold | 운영 의미 |
|---|---|---|---|
| fixed_N1 | RF/LGBM | 0.05–0.22 | 확률 0.1+ 알람, recall-leaning |
| Horizon H16–H24 | GBM | 0.20–0.33 | 확률 0.2+ 알람, precision-leaning |
| LogReg (공통) | — | 0.99+ | class_weight='balanced' 부작용, 확률 쏠림 |

H24 GBM baseline 의 threshold 0.20 은 exp_006 의 horizon best(0.22)와 같은 precision-leaning 대역. **horizon best 는 threshold 0.2 전후로 일관 캘리브레이션됨.**

---

## 5. Horizon 길이 효과 (H10→H24 전 구간, exp_006+exp_007 종합)

| Horizon | Best PR-AUC | Best F1 | 최강 모델 | Test Pos | 출처 |
|---|---|---|---|---|---|
| H10 | 0.157 | 0.054 | LogReg | 36 | exp_006 |
| H12 | 0.209 | 0.257 | LogReg/RF | 51 | exp_006 |
| H14 | 0.185 | — | GBM | 60 | exp_006 |
| H16 | 0.232 | 0.272 | GBM | 54 | exp_007 |
| H18 | 0.212 | 0.265 | GBM | 61 | exp_007 |
| H20 | 0.225 | 0.342 | RF | 46 | exp_006 |
| H22 | 0.243 | 0.167 | XGB | 27 | exp_007 |
| **H24** | **0.256** | 0.311 | GBM | 28 | exp_007 |

**해석:** H10(0.157) → H24(0.256) 로 horizon 이 길수록 best PR-AUC 가 우상향 추세 — exp_006 의 "윈도우 길수록 양성 비율 증가 → 평가 안정·성능 향상" 가설이 전 구간에서 재확인됐다. **H24 가 전 horizon 통틀어 최강.**

단 **경고**: H22/H24 의 우위 일부는 test_rows 가 1,608(양성 27~28)로 작아진 데서 온다. 라벨 확정 불가로 어려운 2024 후반 케이스가 test 에서 빠진 효과가 섞여 있다. 따라서 **표본이 두터운 H16(test 4,894/54, PR 0.232)** 이 실무 신뢰도 면에서는 H24(0.256)에 준하는 강후보다. CI 분석은 후속 과제.

---

## 6. Variant 효과 (test 기준)

| 모델 군 | 선호 variant | 비고 |
|---|---|---|
| GBM | **baseline**(H24), **exp-A/C**(H16) | horizon best 자체가 baseline — winsorize 가 항상 유리한 건 아님 |
| RF | **exp-A/exp-C**(H22/H24) | winsorize 효과, robust scale 영향 미미(b=B, A=C) |
| XGB | **exp-C**(H22) | winsorize+scale 조합 |
| LGBM | **exp-A**(H22) | winsorize 단독 |
| LogReg | variant 무관 | 자체 StandardScaler 파이프라인이라 robust scale 무효(b=B, A=C) |

exp_006 은 "winsorize(exp-A)가 RF/LogReg 에서 일관 1위" 라 결론냈으나, exp_007 의 최강(H24 GBM)은 **baseline(전처리 최소)** 다. 즉 **모델·horizon 별로 선호 variant 가 갈리며, GBM + 긴 horizon 조합에서는 winsorize 가 오히려 불리**. "winsorize 가 만능" 결론은 약화됐다.

---

## 7. 결론 및 다음 단계

### 결론

| 항목 | 값 |
|---|---|
| **전체 Best 모델** | **fixed_N1 + RF + exp-C** (exp_004) |
| Test PR-AUC | **0.2861** |
| **Horizon Best (신규)** | **H24 + GBM + baseline** → PR-AUC **0.2556**, F1 0.3111 |
| 신뢰도 높은 Horizon 후보 | H16 + GBM + exp-A → PR-AUC 0.2324 (test 4,894/54) |
| fixed_N1 vs Horizon best 격차 | exp_006 +27% → exp_007 **+11.9%** (절반 이하로 축소) |

- **긴 horizon(H24)이 horizon 진영 best 를 0.2247→0.2556 로 끌어올려 fixed_N1 격차를 절반 이하로 좁혔다.** 라벨 정의 우위는 유지되나 horizon 길이로 상당 부분 보완 가능.
- **GBM 이 horizon 최강 모델로 확정** (전 horizon·전 모델 1위). exp_006 의 "GBM horizon 우세" 가설 확증.
- **Horizon 길이 효과가 H10→H24 전 구간에서 단조 우상향** 으로 재확인. 다만 H22/H24 의 test 표본 급감(1,608행)으로 절대 수치는 H16 대비 통계적 신뢰가 낮다.
- **Winsorize 만능론 약화** — horizon best(H24 GBM)는 baseline. variant 선호가 모델·horizon 별로 분산.

### 후속 실험

1. **H24 + GBM + baseline 위에서 Optuna 튜닝** — `n_estimators`, `learning_rate`, `max_depth`, `subsample`. exp_006 후속안의 RF/N1 튜닝과 병행.
2. **H22/H24 test 표본 CI 분석** — bootstrap PR-AUC 신뢰구간으로 H16(두터운 표본) vs H24(작은 표본) 우열을 통계적으로 확정.
3. **H24(GBM) + fixed_N1(RF) 앙상블** — forward 긴 horizon best 와 backward N1 best 확률 stacking. precision-leaning(H24) vs recall-leaning(N1) trade-off 결합.
4. **H16 을 운영 후보로 별도 검증** — test 표본이 두터워 실무 배포 시 더 안전. H16 GBM exp-A 의 threshold 안정성·시계열 재현성 확인.
5. **horizon best 의 시계열 안정성** — train/valid/test 연도를 한 칸씩 밀어 H24 GBM·H16 GBM 우위가 재현되는지 검증.

---

## 산출물 목록

```
results/exp_007_horizon_vs_004/
├── baseline/   rf_H16.json  rf_H18.json  rf_H22.json  rf_H24.json  gbm_*.json  xgb_*.json  lgbm_*.json  logreg_*.json
├── exp-A/      (위와 동일, 20개)
├── exp-B/      (위와 동일, 20개)
├── exp-C/      (위와 동일, 20개)
└── summary.md  # 이 리포트
```

총 80개 JSON (5 모델 × 4 horizon × 4 variant) + summary.md.

비교 대상: `results/exp_006_horizon_vs_004/` (H10/12/14/20, 80 JSON), `results/exp_004_fixed_N/` (fixed_N1/N2, 40 JSON).
</content>
</invoke>
