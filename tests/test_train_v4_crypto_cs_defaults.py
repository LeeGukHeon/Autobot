from autobot.models.train_v4_crypto_cs import TrainV4CryptoCsOptions


def test_walk_forward_windows_default_is_four() -> None:
    assert TrainV4CryptoCsOptions.__dataclass_fields__["walk_forward_windows"].default == 4
