import pytest

from rulebricks_spark._batch_policy import BatchPolicy


def simulate(policy, durations, scale=1, failure_at=None):
    pending = []
    outstanding = 0
    history = []
    for iteration in range(10000):
        while True:
            slot = policy.claim(policy.rows - outstanding)
            if slot is None:
                break
            pending.append(slot)
            outstanding += slot.capacity
            assert outstanding <= policy.rows
            assert len(pending) <= 2 * policy.max_requests  # old and new windows can coexist
        if not pending:
            raise AssertionError("Admission deadlock")
        slot = pending.pop(0)
        outstanding -= slot.capacity
        stable = all(s.generation == policy.generation for s in pending)
        policy.finish(
            slot,
            rows=slot.capacity,
            seconds=(durations(policy) if callable(durations) else durations[policy.width]) * scale,
            correct=iteration != failure_at,
            stable=stable,
        )
        history.append(policy.width)
        if policy.phase == "frozen" and all(s.generation == policy.generation for s in pending):
            return history
    raise AssertionError("Exploration did not terminate")


@pytest.mark.parametrize("scale", [0.001, 1, 1000])
def test_accepts_a_repeatable_gain_then_rejects_a_worse_wider_window(scale):
    policy = BatchPolicy(16, 4)
    simulate(policy, {4: 10, 8: 7, 16: 9}, scale)
    assert policy.best_width == 8 and policy.width == 8 and policy.phase == "frozen"
    assert any(e["phase"] == "confirmation" for e in policy.events)


@pytest.mark.parametrize("rows", [1, 3, 4, 7, 16, 127, 1024])
def test_arbitrary_budgets_are_bounded_across_transitions(rows):
    policy = BatchPolicy(rows, min(5, rows))
    simulate(policy, {i: float(i) for i in range(1, 33)})
    assert policy.width == policy.best_width


def test_failure_cannot_train_a_faster_plan():
    policy = BatchPolicy(16, 4)
    simulate(policy, {4: 10, 8: 1, 16: 0.1}, failure_at=12)
    assert policy.width == policy.best_width == 4 and policy.phase == "frozen"


def test_confirmation_rejects_an_improvement_explained_by_changing_work():
    policy = BatchPolicy(16, 4)
    pending = []
    outstanding = 0
    for _ in range(1000):
        while True:
            slot = policy.claim(policy.rows - outstanding)
            if slot is None:
                break
            pending.append(slot)
            outstanding += slot.capacity
        slot = pending.pop(0)
        outstanding -= slot.capacity
        policy.finish(
            slot,
            rows=slot.capacity,
            seconds={"baseline": 10, "trial": 7, "confirmation": 6}[policy.phase],
            stable=all(s.generation == policy.generation for s in pending),
        )
        if policy.phase == "frozen":
            break
    else:
        raise AssertionError("Exploration did not finish")
    assert policy.width == policy.best_width == 4
    assert any(e["phase"] == "confirmation" for e in policy.events)


def test_a_single_easy_candidate_window_cannot_select_a_plan():
    policy = BatchPolicy(16, 4, max_probes=1)
    simulate(policy, lambda p: 6 if p.phase == "trial" else 10)
    assert policy.best_width == policy.width == 4
    assert any(e["phase"] == "trial_repeat" for e in policy.events)


def test_mean_gain_inside_observed_variation_is_rejected():
    policy = BatchPolicy(16, 4, max_probes=1)

    def timings(p):
        blocks = [6, 12, 6, 12] if p.width == 4 else [4.8, 9.6, 4.8, 9.6]
        return blocks[min(len(p.block_scores), 3)]

    simulate(policy, timings)
    measurements = [e for e in policy.events if e.get("event") == "measurement"]
    baseline, trial = measurements[:2]
    assert trial["score"] < baseline["score"] * 0.9
    assert trial["block_upper_quartile"] > baseline["block_lower_quartile"]
    assert policy.best_width == 4


@pytest.mark.parametrize("scale", [0.001, 1, 1000])
def test_repeatable_gain_with_modest_variation_is_accepted(scale):
    policy = BatchPolicy(16, 4, max_probes=1)

    def timings(p):
        blocks = [9.8, 10.2, 10.1, 9.9] if p.width == 4 else [6.8, 7.1, 7.0, 7.2]
        return blocks[min(len(p.block_scores), 3)]

    simulate(policy, timings, scale=scale)
    assert policy.best_width == policy.width == 8
    assert any(e.get("phase") == "trial_repeat" for e in policy.events)


def test_larger_batches_can_use_a_generous_ceiling():
    policy = BatchPolicy(16, 16)
    assert policy.width == 4  # Initial overlap is separate from the ceiling.
    simulate(policy, {4: 10, 2: 7, 1: 6})
    assert policy.best_width == 1 and policy.probes == 2


def test_rejected_larger_batch_still_allows_a_smaller_batch_probe():
    policy = BatchPolicy(16, 16)
    simulate(policy, {4: 10, 2: 12, 8: 7})
    assert policy.best_width == 8 and policy.probes == 2


def test_cold_first_requests_are_excluded_from_learning():
    policy = BatchPolicy(16, 4, max_probes=1)
    simulate(policy, lambda p: 100 if p.generation == 0 and p.warmup_rows else 10)
    assert policy.best_width == 4
    measurements = [e for e in policy.events if e.get("event") == "measurement"]
    assert measurements[0]["score"] == 10


def test_byte_constraints_do_not_restart_exploration_after_a_failure():
    policy = BatchPolicy(16, 16)
    simulate(policy, {4: 10, 2: 7, 1: 6}, failure_at=12)
    assert policy.phase == "frozen"
    assert policy.constrain_batch_rows(1)
    assert policy.phase == "frozen" and policy.best_width == policy.width == 16
    assert not policy.constrain_batch_rows(1)
