from functools import lru_cache

from web3 import Web3

from .config import get_settings


@lru_cache
def get_web3() -> Web3:
    settings = get_settings()
    provider = Web3.HTTPProvider(settings.rpc_url)
    return Web3(provider)
