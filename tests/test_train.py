# Tests for model/train.py on a small dataset: the report has the metrics we promise, class weights
# raise recall, the selected model can score, and training is deterministic.

import pytest

import features
import scorer
import train


@pytest.fixture(scope="module")
def trained():
    return train.train(n_txns=20_000, seed=7)


def test_report_has_precision_recall_f1_for_every_model(trained):
    _, report = trained
    assert 0.01 < report["fraud_share"] < 0.03  # FRAUD_RATE=0.02 -> ~2% of txns
    for name in ["logreg_unweighted", "logreg_balanced", "random_forest_balanced"]:
        test_metrics = report["models"][name]["test"]
        assert {"precision", "recall", "f1", "tp", "fp", "fn", "tn"} <= test_metrics.keys()


def test_class_weights_raise_recall(trained):
    _, report = trained
    m = report["models"]
    assert m["logreg_balanced"]["test"]["recall"] > m["logreg_unweighted"]["test"]["recall"]


def test_majority_baseline_catches_nothing_despite_high_accuracy(trained):
    _, report = trained
    base = report["majority_baseline"]
    assert base["recall"] == 0 and base["accuracy"] > 0.95


def test_selected_model_scores_a_txn(trained):
    bundle, report = trained
    assert bundle["name"] == report["selected"] != "logreg_unweighted"
    assert bundle["feature_names"] == features.FEATURE_NAMES
    f = {name: 0.0 for name in features.FEATURE_NAMES}
    assert 0.0 <= scorer.score(bundle, f) <= 1.0


def test_training_is_deterministic(trained):
    _, report = trained
    _, again = train.train(n_txns=20_000, seed=7)
    assert again == report
