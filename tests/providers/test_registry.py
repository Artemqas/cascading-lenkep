from app.enums import CardType, Direction
from app.models import Provider
from app.providers.registry import eligible_providers


async def seed_provider(session, *, id, supported_directions, supported_card_types,
                         min_amount, max_amount, enabled=True, priority=None):
    provider = Provider(
        id=id,
        name=id,
        priority=priority,
        supported_directions=supported_directions,
        supported_card_types=supported_card_types,
        min_amount=min_amount,
        max_amount=max_amount,
        enabled=enabled,
    )
    session.add(provider)
    await session.commit()
    return provider


async def test_eligible_providers_filters_by_direction_card_type_and_amount(db_session):
    await seed_provider(db_session, id="a", supported_directions=["deposit"],
                         supported_card_types=["visa"], min_amount=100, max_amount=10000)
    await seed_provider(db_session, id="b", supported_directions=["withdraw"],
                         supported_card_types=["visa"], min_amount=100, max_amount=10000)
    result = await eligible_providers(db_session, Direction.DEPOSIT, CardType.VISA, 500)
    assert [p.id for p in result] == ["a"]
