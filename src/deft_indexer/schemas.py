from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, HttpUrl


class ContractRegistrationRequest(BaseModel):
    address: str = Field(..., min_length=42, max_length=42)
    network: str = Field(default="ethereum")
    start_block: int = Field(..., ge=0)
    abi_json: str | None = None
    etherscan_url: HttpUrl | None = None


class ContractResponse(BaseModel):
    id: int
    address: str
    network: str
    start_block: int
    active: bool
    created_at: datetime

    class Config:
        from_attributes = True


class EventRecordResponse(BaseModel):
    contract_id: int
    event_name: str
    block_number: int
    log_index: int
    transaction_hash: str
    parsed_data: dict[str, Any]
    raw_data: dict[str, Any]
    created_at: datetime

    class Config:
        from_attributes = True


class ReindexRequest(BaseModel):
    from_block: int = Field(..., ge=0)
    to_block: int | None = Field(None, ge=0)
