from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from deft_indexer.services.abi import ABIResolutionError, resolve_abi


@pytest.mark.asyncio
async def test_resolve_abi_with_inline_json():
    """Test resolve_abi with valid inline JSON ABI."""
    abi_json = '[{"type": "function", "name": "transfer"}]'
    result = await resolve_abi(abi_json, None)
    assert result == abi_json


@pytest.mark.asyncio
async def test_resolve_abi_with_empty_array():
    """Test resolve_abi with empty JSON array."""
    abi_json = "[]"
    result = await resolve_abi(abi_json, None)
    assert result == "[]"


@pytest.mark.asyncio
async def test_resolve_abi_invalid_json():
    """Test resolve_abi with invalid JSON raises ABIResolutionError."""
    abi_json = "not valid json"
    with pytest.raises(ABIResolutionError):
        await resolve_abi(abi_json, None)


@pytest.mark.asyncio
async def test_resolve_abi_json_not_array():
    """Test resolve_abi with JSON object instead of array raises error."""
    abi_json = '{"type": "function"}'
    with pytest.raises(ABIResolutionError) as exc_info:
        await resolve_abi(abi_json, None)
    assert "must be a JSON array" in str(exc_info.value)


@pytest.mark.asyncio
async def test_resolve_abi_from_etherscan_success():
    """Test resolve_abi fetches ABI from Etherscan URL successfully."""
    etherscan_url = (
        "https://api.etherscan.io/api?module=contract&action=getabi&address=0x123"
    )
    abi_data = '[{"type": "event", "name": "Transfer"}]'

    mock_response = MagicMock()
    mock_response.json = MagicMock(return_value={"status": "1", "result": abi_data})
    mock_response.raise_for_status = MagicMock()

    with patch("httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__.return_value.get = AsyncMock(
            return_value=mock_response
        )
        result = await resolve_abi(None, etherscan_url)
        assert result == abi_data


@pytest.mark.asyncio
async def test_resolve_abi_from_etherscan_invalid_response():
    """Test resolve_abi with malformed Etherscan response raises error."""
    etherscan_url = (
        "https://api.etherscan.io/api?module=contract&action=getabi&address=0x123"
    )

    mock_response = MagicMock()
    mock_response.json = MagicMock(return_value={"status": "0", "message": "NOTOK"})
    mock_response.raise_for_status = MagicMock()

    with patch("httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__.return_value.get = AsyncMock(
            return_value=mock_response
        )
        with pytest.raises(ABIResolutionError) as exc_info:
            await resolve_abi(None, etherscan_url)
        assert "Invalid ABI payload" in str(exc_info.value)


@pytest.mark.asyncio
async def test_resolve_abi_from_etherscan_http_error():
    """Test resolve_abi handles HTTP errors from Etherscan."""
    etherscan_url = (
        "https://api.etherscan.io/api?module=contract&action=getabi&address=0x123"
    )

    def raise_http_error():
        raise httpx.HTTPStatusError("404", request=MagicMock(), response=MagicMock())

    mock_response = MagicMock()
    mock_response.raise_for_status = raise_http_error

    with patch("httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__.return_value.get = AsyncMock(
            return_value=mock_response
        )
        with pytest.raises(httpx.HTTPStatusError):
            await resolve_abi(None, etherscan_url)


@pytest.mark.asyncio
async def test_resolve_abi_no_sources():
    """Test resolve_abi with neither abi_json nor etherscan_url raises error."""
    with pytest.raises(ABIResolutionError) as exc_info:
        await resolve_abi(None, None)
    assert "required" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_resolve_abi_prefers_inline_json():
    """Test resolve_abi uses abi_json when both sources provided."""
    abi_json = '[{"type": "constructor"}]'
    etherscan_url = (
        "https://api.etherscan.io/api?module=contract&action=getabi&address=0x123"
    )

    # Should not make HTTP request if abi_json is provided
    result = await resolve_abi(abi_json, etherscan_url)
    assert result == abi_json


@pytest.mark.asyncio
async def test_resolve_abi_from_etherscan_result_not_string():
    """Test resolve_abi handles non-string result field in Etherscan response."""
    etherscan_url = (
        "https://api.etherscan.io/api?module=contract&action=getabi&address=0x123"
    )

    mock_response = MagicMock()
    mock_response.json = MagicMock(return_value={"result": 123})  # Not a string
    mock_response.raise_for_status = MagicMock()

    with patch("httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__.return_value.get = AsyncMock(
            return_value=mock_response
        )
        with pytest.raises(ABIResolutionError):
            await resolve_abi(None, etherscan_url)
