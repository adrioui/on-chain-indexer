from __future__ import annotations

import json
from typing import Any

import httpx


class ABIResolutionError(RuntimeError):
    pass


async def resolve_abi(abi_json: str | None, etherscan_url: str | None) -> str:
    if abi_json:
        _validate_json(abi_json)
        return abi_json
    if etherscan_url:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(str(etherscan_url))
            response.raise_for_status()
            data = response.json()
            print("data", data)
            if isinstance(data, dict) and "result" in data:
                result = data["result"]
                if isinstance(result, str):
                    _validate_json(result)
                    return result
            raise ABIResolutionError("Invalid ABI payload from etherscan API")
    raise ABIResolutionError("Either abi_json or etherscan_url is required")


def _validate_json(payload: str) -> list[Any]:
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as e:
        raise ABIResolutionError(f"Invalid JSON: {e}") from e
    if not isinstance(parsed, list):
        raise ABIResolutionError("ABI must be a JSON array")
    return parsed
