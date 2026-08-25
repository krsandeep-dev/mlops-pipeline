from mlops_pipeline.registry import ChampionSnapshot, decide


def _decide(candidate_mae, champion=None, baseline_mae=7.8):
    return decide(
        candidate_mae=candidate_mae,
        baseline_mae=baseline_mae,
        champion=champion,
        candidate_version="2",
    )


def test_bootstraps_when_no_champion_exists():
    decision = _decide(3.5)
    assert decision.promoted
    assert "bootstrapping" in decision.reason


def test_rejects_a_candidate_that_loses_to_the_baseline():
    decision = _decide(9.0)
    assert not decision.promoted
    assert "baseline" in decision.reason


def test_promotes_on_a_clear_improvement():
    decision = _decide(3.0, champion=ChampionSnapshot(version="1", mae=3.5))
    assert decision.promoted


def test_rejects_a_marginal_improvement():
    decision = _decide(3.48, champion=ChampionSnapshot(version="1", mae=3.5))
    assert not decision.promoted
    assert "below the" in decision.reason


def test_rejects_a_regression():
    decision = _decide(4.0, champion=ChampionSnapshot(version="1", mae=3.5))
    assert not decision.promoted
