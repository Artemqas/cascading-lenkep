import pytest

from app.enums import CardType, Direction
from app.stats import correct_result, get_score, laplace_score, record_result


def test_laplace_score_smooths_small_samples():
    assert laplace_score(0, 0) == 0.5
    assert laplace_score(1, 0) == pytest.approx(2 / 3)


async def test_record_and_correct_result(db_session):
    await record_result(db_session, "p1", Direction.DEPOSIT, CardType.VISA, success=False)
    assert await get_score(db_session, "p1", Direction.DEPOSIT, CardType.VISA) == pytest.approx(1 / 3)
    await correct_result(db_session, "p1", Direction.DEPOSIT, CardType.VISA, old_success=False, new_success=True)
    assert await get_score(db_session, "p1", Direction.DEPOSIT, CardType.VISA) == pytest.approx(2 / 3)
