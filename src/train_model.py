"""Train a model that predicts propagation disagreement, tracked by MLflow.

The Orbit Reliability Index encodes hand-designed physics: freshness and
drag sensitivity, weighted 0.55/0.45. This lesson asks whether a model can
LEARN that relationship from the measured pairs, and whether it beats the
hand-crafted index.

    python src/train_model.py

Setup:

- Features are point-in-time correct: exactly what was knowable at the
  earlier element's snapshot (altitude, element age, inclination,
  eccentricity, bstar, source group).
- Target: log1p of the disagreement RATE in km/h, because rates span
  orders of magnitude and the log makes MAE meaningful.
- Validation: GroupKFold grouped by NORAD id. The same object can appear
  in several measured pairs; random splits would leak an object's behavior
  into the test set. Grouping makes every test object unseen during
  training.
- Baseline: predicting the training median, in the same folds. A model is
  only useful if it beats the baseline.

MLflow tracking stores the experiment in a local SQLite database
(git-ignored); inspect it with:

    python -m mlflow ui --backend-store-uri sqlite:///mlflow.db

Honest limitations, logged with the run: 209 pairs from one quiet day is a
small sample; grouped CV is not temporal validation; the environment
factor has no storm data to learn from yet.
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import duckdb
import mlflow
import mlflow.sklearn
import numpy as np
from scipy.stats import spearmanr
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import GroupKFold

ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "data" / "stormtrace.duckdb"
MLFLOW_DB = ROOT / "mlflow.db"
MODEL_REPORT = ROOT / "data" / "reports" / "model_summary.json"

EXPERIMENT = "stormtrace-disagreement"

NUMERIC_FEATURES = [
    "element_age_hours_at_prediction",
    "mean_altitude_km_at_prediction",
    "inclination_degrees_at_prediction",
    "eccentricity_at_prediction",
    "bstar_at_prediction",
]
CATEGORICAL_FEATURES = ["source_group"]


def load_dataset(connection: duckdb.DuckDBPyConnection) -> tuple[np.ndarray, np.ndarray, np.ndarray, list[str]]:
    rows = connection.execute(
        """
        SELECT
            element_age_hours_at_prediction,
            mean_altitude_km_at_prediction,
            inclination_degrees_at_prediction,
            eccentricity_at_prediction,
            bstar_at_prediction,
            source_group,
            km_per_hour,
            norad_catalog_id
        FROM gold_ori_validation_pairs
        WHERE km_per_hour IS NOT NULL
          AND km_per_hour > 0
        """
    ).fetchall()
    if len(rows) < 30:
        print(
            f"Only {len(rows)} usable pairs; at least 30 are required to "
            "train meaningfully. Keep collecting snapshots.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    groups = np.array([row[7] for row in rows])
    rates = np.array([row[6] for row in rows], dtype=float)
    targets = np.log1p(rates)

    feature_names: list[str] = list(NUMERIC_FEATURES)
    matrix_rows: list[list[float]] = []
    group_values = sorted({row[5] for row in rows})
    group_index = {value: position for position, value in enumerate(group_values)}
    for value in group_values:
        feature_names.append(f"group_{value}")

    for row in rows:
        numeric = [float(value) if value is not None else 0.0 for value in row[:5]]
        one_hot = [0.0] * len(group_values)
        one_hot[group_index[row[5]]] = 1.0
        matrix_rows.append(numeric + one_hot)

    return np.array(matrix_rows), targets, groups, feature_names


def main() -> int:
    if not DATABASE.exists():
        print("Run earlier lessons first. The DuckDB database is missing.", file=sys.stderr)
        return 1

    connection = duckdb.connect(str(DATABASE), read_only=True)
    try:
        features, targets, groups, feature_names = load_dataset(connection)
    finally:
        connection.close()

    n_pairs = len(targets)
    n_objects = len(set(groups.tolist()))
    print("StormTrace lesson 21 model training")
    print(f"Training pairs: {n_pairs:,} from {n_objects:,} objects")
    print(f"Target: log1p of disagreement rate (km/h)")
    print()

    model_params = {
        "n_estimators": 150,
        "max_depth": 3,
        "min_samples_leaf": 8,
        "learning_rate": 0.05,
        "subsample": 0.9,
        "random_state": 42,
    }
    model = GradientBoostingRegressor(**model_params)

    splitter = GroupKFold(n_splits=5)
    fold_metrics: list[dict[str, float]] = []
    baseline_metrics: list[dict[str, float]] = []
    all_predictions: list[float] = []
    all_truths: list[float] = []

    for fold, (train_index, test_index) in enumerate(
        splitter.split(features, targets, groups=groups)
    ):
        x_train, x_test = features[train_index], features[test_index]
        y_train, y_test = targets[train_index], targets[test_index]

        fold_model = GradientBoostingRegressor(**model_params)
        fold_model.fit(x_train, y_train)
        predictions = fold_model.predict(x_test)

        baseline = float(np.median(y_train))
        baseline_predictions = np.full_like(y_test, baseline)

        all_predictions.extend(predictions.tolist())
        all_truths.extend(y_test.tolist())

        fold_metrics.append(
            {
                "mae_log": float(np.mean(np.abs(y_test - predictions))),
                "rmse_log": float(math.sqrt(np.mean((y_test - predictions) ** 2))),
                "spearman_rate": float(
                    spearmanr(np.expm1(predictions), np.expm1(y_test)).statistic
                ),
            }
        )
        baseline_metrics.append(
            {
                "mae_log": float(np.mean(np.abs(y_test - baseline_predictions))),
                "rmse_log": float(
                    math.sqrt(np.mean((y_test - baseline_predictions) ** 2))
                ),
                "spearman_rate": 0.0,
            }
        )
        print(
            f"  fold {fold + 1}: MAE(log) {fold_metrics[-1]['mae_log']:.4f} "
            f"vs baseline {baseline_metrics[-1]['mae_log']:.4f}, "
            f"Spearman {fold_metrics[-1]['spearman_rate']:.3f}"
        )

    model.fit(features, targets)
    importance = model.feature_importances_
    ranked = sorted(
        zip(feature_names, importance.tolist()), key=lambda item: -item[1]
    )

    def average(metrics: list[dict[str, float]], key: str) -> float:
        return float(np.mean([metric[key] for metric in metrics]))

    model_mae = average(fold_metrics, "mae_log")
    baseline_mae = average(baseline_metrics, "mae_log")
    model_rmse = average(fold_metrics, "rmse_log")
    baseline_rmse = average(baseline_metrics, "rmse_log")
    model_spearman = average(fold_metrics, "spearman_rate")

    # Out-of-fold overall Spearman (pooled predictions, like the ORI's).
    pooled_spearman = float(
        spearmanr(np.expm1(np.array(all_predictions)), np.expm1(np.array(all_truths))).statistic
    )

    print()
    print("Out-of-fold results (GroupKFold by object):")
    print(f"  Model MAE (log):     {model_mae:.4f}")
    print(f"  Baseline MAE (log):  {baseline_mae:.4f}")
    print(f"  Model RMSE (log):    {model_rmse:.4f}")
    print(f"  Baseline RMSE (log): {baseline_rmse:.4f}")
    print(f"  Pooled Spearman (predicted vs true rate): {pooled_spearman:.3f}")
    print()
    print("Reference: the hand-crafted ORI scores")
    print("  Spearman -0.25 vs rate, -0.43 vs total km (all pairs).")
    print()
    print("Feature importance (top 5):")
    for name, score in ranked[:5]:
        print(f"  {name:<40} {score:.3f}")

    # MLflow's file-based tracking store is in maintenance mode; the
    # SQLite backend is the recommended local option. The database and its
    # artifacts directory are git-ignored.
    mlflow.set_tracking_uri(f"sqlite:///{MLFLOW_DB}")
    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run(run_name="gbr-groupkfold") as run:
        mlflow.log_params(model_params)
        mlflow.log_param("n_pairs", n_pairs)
        mlflow.log_param("n_objects", n_objects)
        mlflow.log_param("cv", "GroupKFold-5-by-norad-id")
        mlflow.log_param("target", "log1p_km_per_hour")
        mlflow.log_param("features", ",".join(feature_names))

        mlflow.log_metric("model_mae_log", model_mae)
        mlflow.log_metric("baseline_mae_log", baseline_mae)
        mlflow.log_metric("model_rmse_log", model_rmse)
        mlflow.log_metric("baseline_rmse_log", baseline_rmse)
        mlflow.log_metric("model_spearman_mean_fold", model_spearman)
        mlflow.log_metric("model_spearman_pooled", pooled_spearman)
        mlflow.log_metric(
            "improvement_over_baseline_mae", baseline_mae - model_mae
        )

        for fold_index, metrics in enumerate(fold_metrics):
            for key, value in metrics.items():
                mlflow.log_metric(f"fold{fold_index + 1}_{key}", value)

        mlflow.log_dict(
            {
                "top_features": [
                    {"feature": name, "importance": round(score, 4)}
                    for name, score in ranked
                ],
                "limitations": [
                    f"Only {n_pairs} pairs from one quiet day; small sample.",
                    "GroupKFold prevents object leakage but is not temporal "
                    "validation.",
                    "No disturbed space-weather period is included, so no "
                    "environment features could be learned.",
                    "The later element is not ground truth; the target is "
                    "public-estimate disagreement.",
                ],
            },
            "feature_importance.json",
        )
        mlflow.sklearn.log_model(
            model,
            name="disagreement_rate_model",
            input_example=features[:5],
        )
        run_id = run.info.run_id

    MODEL_REPORT.parent.mkdir(parents=True, exist_ok=True)
    MODEL_REPORT.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "experiment": EXPERIMENT,
                "pairs": n_pairs,
                "objects": n_objects,
                "model_mae_log": round(model_mae, 4),
                "baseline_mae_log": round(baseline_mae, 4),
                "model_spearman_pooled": round(pooled_spearman, 4),
                "ori_reference_spearman_vs_rate": -0.2519,
                "ori_reference_spearman_vs_total": -0.4277,
                "top_features": [
                    {"feature": name, "importance": round(score, 4)}
                    for name, score in ranked[:5]
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print()
    print(f"MLflow run logged: {run_id}")
    print(f"Tracking database: {MLFLOW_DB.relative_to(ROOT)}")
    print("Inspect with: python -m mlflow ui --backend-store-uri sqlite:///mlflow.db")
    print(f"Report: {MODEL_REPORT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
