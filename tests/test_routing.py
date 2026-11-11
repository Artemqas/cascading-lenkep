from app.enums import CardType, Direction
from app.models import Provider
from app.routing import order_providers
from app.stats import record_result


def make_provider(*, id, priority):
    return Provider(
        id=id,
        name=id,
        priority=priority,
        supported_directions=["deposit"],
        supported_card_types=["visa"],
        min_amount=0,
        max_amount=100000,
        enabled=True,
    )


async def test_manual_priority_providers_always_rank_before_adaptive(db_session):
    p_low_priority = make_provider(id="x", priority=2)
    p_high_priority = make_provider(id="y", priority=1)
    p_adaptive_strong = make_provider(id="z", priority=None)
    await record_result(db_session, "z", Direction.DEPOSIT, CardType.VISA, success=True)
    ordered = await order_providers(db_session, [p_low_priority, p_high_priority, p_adaptive_strong],
                                     Direction.DEPOSIT, CardType.VISA)
    assert [p.id for p in ordered] == ["y", "x", "z"]


async def test_adaptive_only_providers_rank_by_score(db_session):
    p_weak = make_provider(id="weak", priority=None)
    p_strong = make_provider(id="strong", priority=None)
    await record_result(db_session, "strong", Direction.DEPOSIT, CardType.VISA, success=True)
    await record_result(db_session, "weak", Direction.DEPOSIT, CardType.VISA, success=False)
    ordered = await order_providers(db_session, [p_weak, p_strong], Direction.DEPOSIT, CardType.VISA)
    assert [p.id for p in ordered] == ["strong", "weak"]
