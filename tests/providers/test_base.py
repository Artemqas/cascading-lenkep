from dataclasses import FrozenInstanceError

import pytest

from app.providers.base import AttemptOutcome, OutcomeKind


def test_attempt_outcome_is_frozen_and_typed():
    outcome = AttemptOutcome(kind=OutcomeKind.DECLINED_FINAL, decline_code="insufficient_funds")
    assert outcome.kind == OutcomeKind.DECLINED_FINAL
    assert outcome.decline_code == "insufficient_funds"
    with pytest.raises(FrozenInstanceError):
        outcome.decline_code = "other"
