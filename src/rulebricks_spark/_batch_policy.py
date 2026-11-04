"""Conservative, bounded batch-size exploration under a fixed row allowance."""

from collections import deque
from dataclasses import dataclass
from statistics import quantiles


@dataclass(frozen=True)
class Slot:
    generation: int
    capacity: int


@dataclass(frozen=True)
class Measurement:
    score: float
    lower: float
    upper: float


class BatchPolicy:
    """Compare repeated configurations without assuming a flow runtime.

    Score is reserved row-seconds per correctly completed row. A-B-A-B windows
    guard against a single lucky candidate window or a drifting baseline. Each
    window also retains the central half of its block scores: overlapping bands
    reject a candidate even when its overall mean looks better. These are
    conservative empirical guards, not independent samples or confidence bounds.
    """

    def __init__(
        self,
        rows,
        fitted_batch_rows,
        max_requests=32,
        improvement=0.10,
        max_probes=2,
        initial_requests=4,
    ):
        self.rows = rows
        self.max_requests = min(rows, max_requests)
        self.minimum_width = min(
            self.max_requests, max(1, (rows + fitted_batch_rows - 1) // fitted_batch_rows)
        )
        # The caller's byte ceiling is not a target request size. Start with
        # overlap, leaving room to explore both larger and smaller batches.
        self.width = min(self.max_requests, max(initial_requests, self.minimum_width))
        self.best_width = self.width
        self.improvement = improvement
        self.max_probes = max_probes
        self.probes = 0
        self.generation = -1
        self.phase = "baseline"
        self.baseline = None
        self.repeated_baseline = None
        self.trial = None
        self.trial_width = None
        self.tested_widths = {self.width}
        self.events = []
        self._set_width(self.width)

    def _set_width(self, width, warmup=True):
        self.width = width
        self.generation += 1
        base, extra = divmod(self.rows, width)
        self.ready = deque(Slot(self.generation, base + int(i < extra)) for i in range(width))
        self.warmup_rows = self.rows if warmup else 0
        self.completed_rows = self.completions = 0
        self.row_seconds = 0.0
        self.minimum_rows = 4 * self.rows
        self.block_rows = self.block_completions = 0
        self.block_seconds = 0.0
        self.block_scores = []
        self.events.append({"generation": self.generation, "width": width, "phase": self.phase})

    def claim(self, available_rows):
        # Old requests retain their allowances through a plan change.
        for slot in self.ready:
            if slot.capacity <= available_rows:
                self.ready.remove(slot)
                return slot
        return None

    def freeze(self):
        self.phase = "frozen"
        if self.width != self.best_width:
            self._set_width(self.best_width)

    def constrain_batch_rows(self, fitted_rows):
        """Accommodate larger payloads without restarting a stopped search."""
        required = min(self.max_requests, (self.rows + fitted_rows - 1) // fitted_rows)
        if required <= self.minimum_width:
            return False
        self.minimum_width = required
        self.best_width = max(self.width, required)
        self.tested_widths.add(self.best_width)
        if self.phase != "frozen":
            self.phase = "baseline"
        self._set_width(self.best_width)
        return True

    def _candidates(self):
        # First try amortizing request overhead, then additional concurrency.
        return [
            width
            for width in (
                max(self.minimum_width, (self.best_width + 1) // 2),
                min(self.max_requests, 2 * self.best_width),
            )
            if width != self.best_width and width not in self.tested_widths
        ]

    def _better(self, candidate, *baselines):
        margin = 1 - self.improvement
        return all(
            candidate.score < baseline.score * margin and candidate.upper < baseline.lower * margin
            for baseline in baselines
        )

    def _reject_trial(self):
        self.phase = (
            "baseline" if self.probes < self.max_probes and self._candidates() else "frozen"
        )
        self._set_width(self.best_width)

    def finish(
        self, slot, *, rows, seconds, correct=True, retried=False, stable=True, partial_tail=False
    ):
        if slot.generation == self.generation:
            self.ready.append(slot)
        if not correct or retried:
            self.freeze()
            return
        if (
            self.phase == "frozen"
            or slot.generation != self.generation
            or not stable
            or partial_tail
            or rows <= 0
            or seconds <= 0
        ):
            return
        if self.warmup_rows > 0:
            self.warmup_rows = max(0, self.warmup_rows - rows)
            return
        cost = slot.capacity * seconds
        self.completed_rows += rows
        self.completions += 1
        self.row_seconds += cost
        self.block_rows += rows
        self.block_completions += 1
        self.block_seconds += cost
        # Pool completions into blocks instead of treating overlapping HTTP
        # requests as independent timing observations. Keep slow requests in
        # both the overall score and their block; do not trim tail latency.
        if self.block_rows < self.rows or self.block_completions < 4:
            return
        self.block_scores.append(self.block_seconds / self.block_rows)
        self.block_rows = self.block_completions = 0
        self.block_seconds = 0.0
        if self.completed_rows < self.minimum_rows or len(self.block_scores) < 4:
            return
        lower, _, upper = quantiles(self.block_scores, n=4, method="inclusive")
        measurement = Measurement(self.row_seconds / self.completed_rows, lower, upper)
        self.events.append(
            {
                "event": "measurement",
                "phase": self.phase,
                "width": self.width,
                "score": measurement.score,
                "block_lower_quartile": lower,
                "block_upper_quartile": upper,
                "blocks": len(self.block_scores),
                "rows": self.completed_rows,
                "samples": self.completions,
            }
        )
        if self.phase == "baseline":
            self.baseline = measurement
            candidates = self._candidates()
            if not candidates or self.probes >= self.max_probes:
                self.freeze()
                return
            self.trial_width = candidates[0]
            self.tested_widths.add(self.trial_width)
            self.probes += 1
            self.phase = "trial"
            self._set_width(self.trial_width)
        elif self.phase == "trial":
            self.trial = measurement
            if not self._better(self.trial, self.baseline):
                self._reject_trial()
            else:
                self.phase = "confirmation"
                self._set_width(self.best_width)
        elif self.phase == "confirmation":
            self.repeated_baseline = measurement
            if not self._better(self.trial, self.baseline, self.repeated_baseline):
                self._reject_trial()
            else:
                self.phase = "trial_repeat"
                self._set_width(self.trial_width)
        else:
            if self._better(measurement, self.baseline, self.repeated_baseline):
                self.best_width = self.trial_width
                self.phase = (
                    "baseline" if self.probes < self.max_probes and self._candidates() else "frozen"
                )
                self._set_width(self.best_width)
            else:
                self._reject_trial()
