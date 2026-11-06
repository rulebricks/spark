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
    baseline = next(e for e in measurements if e["phase"] == "baseline")
    trial = next(e for e in measurements if e["phase"] == "trial")
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
    simulate(policy, {4: 10, 2: 12, 8: 7, 16: 9})
    assert policy.best_width == 8 and policy.probes == 3


@pytest.mark.parametrize("scale", [0.001, 1, 1000])
def test_rejected_larger_batches_do_not_stop_further_repeatable_improvements(scale):
    policy = BatchPolicy(16, 16)
    simulate(policy, {2: 12, 4: 10, 8: 7, 16: 5}, scale=scale)
    assert policy.best_width == policy.width == 16
    assert policy.phase == "frozen"
    assert policy.trial_admitted_rows * 9 <= policy.baseline_completed_rows


def test_cold_first_requests_are_excluded_from_learning():
    policy = BatchPolicy(16, 4, max_probes=1)
    simulate(policy, lambda p: 100 if p.generation == 0 and p.warmup_rows else 10)
    assert policy.best_width == 4
    measurements = [e for e in policy.events if e.get("event") == "measurement"]
    assert measurements[0]["score"] == 10


def test_compact_start_can_confirm_one_improvement_without_waiting_for_row_credit():
    policy = BatchPolicy(1000, 1000, initial_requests=2, startup_trial=True)
    simulate(policy, {1: 6, 2: 10, 4: 12})
    assert policy.best_width == policy.width == 1
    assert policy.startup_trial_rows == 12 * policy.rows
    assert (
        policy.trial_admitted_rows
        <= policy.startup_trial_rows + policy.baseline_completed_rows // 9
    )
    accepted = next(e for e in policy.events if e.get("phase") == "trial_repeat")
    assert accepted["width"] == 1


def test_startup_comparison_rejects_a_false_gain_from_easier_work():
    policy = BatchPolicy(1000, 1000, initial_requests=2, startup_trial=True, max_probes=1)
    simulate(policy, lambda p: 6 if p.phase == "trial" else 10)
    assert policy.best_width == policy.width == 2
    assert (
        policy.trial_admitted_rows
        <= policy.startup_trial_rows + policy.baseline_completed_rows // 9
    )


def test_startup_failure_restores_the_seeded_plan():
    policy = BatchPolicy(1000, 1000, initial_requests=2, startup_trial=True)
    pending = []
    outstanding = 0
    for _ in range(100):
        while (slot := policy.claim(policy.rows - outstanding)) is not None:
            pending.append(slot)
            outstanding += slot.capacity
        slot = pending.pop(0)
        outstanding -= slot.capacity
        policy.finish(
            slot,
            rows=slot.capacity,
            seconds=1,
            retried=slot.exploratory,
            stable=all(s.generation == policy.generation for s in pending),
        )
        if policy.phase == "frozen":
            break
    assert policy.best_width == policy.width == 2 and policy.phase == "frozen"
    assert policy.trial_admitted_rows > 0


def test_changed_payload_mix_can_relearn_after_settling_without_new_trial_credit():
    policy = BatchPolicy(16, 16, startup_trial=True)
    simulate(policy, {1: 6, 2: 7, 4: 10})
    assert policy.best_width == 1
    old_probes, old_credit = policy.probes, policy.startup_trial_rows
    assert policy.reconsider()
    assert policy.best_width == 1 and policy.probes == old_probes
    assert policy.startup_trial_rows == old_credit
    simulate(policy, {1: 12, 2: 8, 4: 5, 8: 6})
    assert policy.best_width == 4
    assert policy.trial_admitted_rows <= old_credit + policy.baseline_completed_rows // 9


def test_changed_payload_mix_does_not_restart_tuning_after_a_retry():
    policy = BatchPolicy(16, 16, startup_trial=True)
    slot = policy.claim(16)
    policy.finish(slot, rows=slot.capacity, seconds=1, retried=True)
    generation = policy.generation
    assert not policy.reconsider()
    assert policy.phase == "frozen" and policy.generation == generation


def test_internal_tuning_diagnostics_stay_bounded_across_changing_input():
    policy = BatchPolicy(16, 16)
    for _ in range(1000):
        policy.reconsider()
    assert len(policy.events) <= 128
    assert policy.best_width == 4 and policy.probes == 0


def test_byte_constraints_do_not_restart_exploration_after_a_failure():
    policy = BatchPolicy(16, 16)
    simulate(policy, {4: 10, 2: 7, 1: 6}, failure_at=12)
    assert policy.phase == "frozen"
    assert policy.constrain_batch_rows(1)
    assert policy.phase == "frozen" and policy.best_width == policy.width == 16
    assert not policy.constrain_batch_rows(1)


@pytest.mark.parametrize("rows", [1, 3, 7, 16, 127, 1024])
@pytest.mark.parametrize("startup_trial", [False, True])
def test_learning_admissions_are_bounded_at_every_claim(rows, startup_trial):
    import random

    rng = random.Random(171)
    policy = BatchPolicy(rows, rows, startup_trial=startup_trial)
    pending = []
    outstanding = 0
    exploratory = 0
    baseline = 0
    for _ in range(15000):
        while True:
            slot = policy.claim(rows - outstanding)
            if slot is None:
                break
            if slot.exploratory:
                exploratory += slot.capacity
            assert exploratory <= baseline // 9 + policy.startup_trial_rows
            pending.append(slot)
            outstanding += slot.capacity
            assert outstanding <= rows
        assert pending
        slot = pending.pop(rng.randrange(len(pending)))
        outstanding -= slot.capacity
        if not slot.exploratory:
            baseline += slot.capacity
        policy.finish(
            slot,
            rows=slot.capacity,
            seconds=slot.capacity**0.5,
            stable=all(s.generation == policy.generation for s in pending),
        )
    assert exploratory == policy.trial_admitted_rows
    if rows > 3:
        assert exploratory > 0


def test_finite_job_does_not_spend_most_of_its_work_testing():
    policy = BatchPolicy(16, 16)
    for _ in range(64):
        slot = policy.claim(16)
        assert not slot.exploratory
        policy.finish(slot, rows=slot.capacity, seconds=1)
    assert policy.baseline_completed_rows == 256
    assert policy.trial_admitted_rows == 0
    assert policy.probes == 0


def test_budget_exhaustion_falls_back_without_waiting_for_a_trial():
    policy = BatchPolicy(16, 16)
    policy.phase = "trial"
    policy._set_width(2)
    slot = policy.claim(4)
    assert slot is None  # The outstanding old requests retain their allowance.
    slot = policy.claim(8)
    assert policy.phase == "frozen"
    assert slot.capacity == 4 and not slot.exploratory
    assert policy.width == policy.best_width == 4


def test_retry_does_not_earn_learning_credit():
    policy = BatchPolicy(16, 16)
    slot = policy.claim(16)
    policy.finish(slot, rows=4, seconds=1, retried=True)
    assert policy.baseline_completed_rows == policy.trial_admitted_rows == 0
    assert policy.phase == "frozen"


@pytest.mark.parametrize("failure", [{"correct": False}, {"retried": True}])
def test_candidate_failure_restores_baseline_without_earning_credit(failure):
    policy = BatchPolicy(16, 16)
    for _ in range(10000):
        slot = policy.claim(16)
        if slot.exploratory:
            before = policy.baseline_completed_rows
            policy.finish(slot, rows=slot.capacity, seconds=0.001, **failure)
            assert policy.phase == "frozen" and policy.width == policy.best_width == 4
            assert policy.baseline_completed_rows == before
            assert not policy.claim(16).exploratory
            break
        policy.finish(slot, rows=slot.capacity, seconds=1)
    else:
        raise AssertionError("Never tested a candidate plan")
