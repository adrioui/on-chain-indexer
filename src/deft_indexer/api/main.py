from __future__ import annotations

from contextlib import asynccontextmanager

import structlog
from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select
from sqlalchemy.orm import Session
from web3 import Web3

from ..config import get_settings
from ..db import get_session, init_db
from ..logging import configure_logging
from ..models import Contract, EventRecord
from ..schemas import (
    ContractRegistrationRequest,
    ContractResponse,
    EventRecordResponse,
    ReindexRequest,
)
from ..services.abi import ABIResolutionError, resolve_abi
from ..tasks import index_contract_events
from ..web3_provider import get_web3

settings = get_settings()
log = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app):
    # Startup
    configure_logging()
    init_db()
    yield


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Constants
MAX_REQUEST_BODY_SIZE = 512 * 1024  # 512 KB


@app.middleware("http")
async def limit_request_body_size(request: Request, call_next):
    """Reject requests with bodies larger than 512KB."""
    content_length = request.headers.get("content-length")

    # Reject compressed uploads to avoid bombs
    if request.headers.get("content-encoding"):
        log.warning(
            "compressed_upload_rejected",
            encoding=request.headers.get("content-encoding"),
            path=request.url.path,
        )
        return Response(
            content="Compressed uploads not supported",
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
        )

    if content_length:
        content_length_int = int(content_length)
        if content_length_int > MAX_REQUEST_BODY_SIZE:
            log.warning(
                "request_body_too_large",
                content_length=content_length_int,
                max_size=MAX_REQUEST_BODY_SIZE,
                path=request.url.path,
            )
            return Response(
                content="Request body too large",
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            )
    else:
        # Missing Content-Length - enforce limit by rejecting
        if request.method in ["POST", "PUT", "PATCH"]:
            log.warning("missing_content_length", path=request.url.path)
            return Response(
                content="Content-Length header required",
                status_code=status.HTTP_411_LENGTH_REQUIRED,
            )

    response = await call_next(request)
    return response


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


def _validate_contract(address: str, network: str) -> None:
    web3 = get_web3()
    if not web3.is_checksum_address(address):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid contract address"
        )
    code = web3.eth.get_code(address)
    if not code or code == b"":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Contract code not found on chain",
        )


@app.post(
    "/watch", response_model=ContractResponse, status_code=status.HTTP_201_CREATED
)
async def register_contract(
    payload: ContractRegistrationRequest,
    request: Request,
    session: Session = Depends(get_session),
) -> ContractResponse:
    # Apply rate limiting: 5 requests per 60 seconds per IP
    from ..services.api_rate_limiter import rate_limit_middleware

    rate_limit_middleware(request, route="watch", max_requests=5, window_seconds=60)
    try:
        checksum_address = Web3.to_checksum_address(payload.address)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid contract address",
        ) from exc

    _validate_contract(checksum_address, payload.network)
    existing = (
        session.execute(select(Contract).where(Contract.address == checksum_address))
        .scalars()
        .first()
    )
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="Contract already registered"
        )

    try:
        abi = await resolve_abi(payload.abi_json, payload.etherscan_url)
    except ABIResolutionError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc

    contract = Contract(
        address=checksum_address,
        network=payload.network,
        abi=abi,
        start_block=payload.start_block,
    )
    session.add(contract)
    session.commit()
    session.refresh(contract)

    # Apply finality when enqueueing initial indexing task
    web3 = get_web3()
    head = web3.eth.block_number
    safe_head = max(0, head - settings.finality_blocks)

    # Use dedup when enqueueing
    from ..services.enqueue_dedup import enqueue_with_dedup

    enqueue_with_dedup(contract.id, from_block=payload.start_block, to_block=safe_head)

    log.info("contract_registered", contract_id=contract.id, address=contract.address)
    return ContractResponse.model_validate(contract)


@app.get("/contracts", response_model=list[ContractResponse])
def list_contracts(session: Session = Depends(get_session)) -> list[ContractResponse]:
    contracts = session.execute(select(Contract)).scalars().all()
    return [ContractResponse.model_validate(obj) for obj in contracts]


@app.get("/contracts/{contract_id}/events", response_model=list[EventRecordResponse])
def list_events(
    contract_id: int, session: Session = Depends(get_session)
) -> list[EventRecordResponse]:
    events = (
        session.execute(
            select(EventRecord)
            .where(EventRecord.contract_id == contract_id)
            .limit(1000)
        )
        .scalars()
        .all()
    )
    return [EventRecordResponse.model_validate(obj) for obj in events]


@app.post("/contracts/{contract_id}/reindex")
def reindex_contract(
    contract_id: int,
    payload: ReindexRequest,
    request: Request,
    session: Session = Depends(get_session),
) -> dict:
    # Apply rate limiting: 10 requests per 60 seconds per IP
    from ..services.api_rate_limiter import rate_limit_middleware

    rate_limit_middleware(request, route="reindex", max_requests=10, window_seconds=60)
    contract = (
        session.execute(select(Contract).where(Contract.id == contract_id))
        .scalars()
        .first()
    )
    if not contract:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Contract not found"
        )

    # Apply finality to manual reindex
    web3 = get_web3()
    head = web3.eth.block_number
    safe_head = max(0, head - settings.finality_blocks)

    # Cap to_block at safe_head if not specified or beyond finality
    to_block = payload.to_block if payload.to_block is not None else safe_head
    if to_block > safe_head:
        log.warning(
            "reindex_capped_to_safe_head",
            contract_id=contract_id,
            requested=to_block,
            safe_head=safe_head,
        )
        to_block = safe_head

    # Use dedup when enqueueing
    from ..services.enqueue_dedup import enqueue_with_dedup

    enqueued = enqueue_with_dedup(
        contract_id, from_block=payload.from_block, to_block=to_block
    )

    return {
        "status": "enqueued" if enqueued else "already_queued",
        "contract_id": contract_id,
        "from_block": payload.from_block,
        "to_block": to_block,
        "safe_head": safe_head,
    }


@app.get("/metrics")
def metrics() -> Response:
    from prometheus_client import generate_latest

    payload = generate_latest()
    return Response(content=payload, media_type="text/plain")
