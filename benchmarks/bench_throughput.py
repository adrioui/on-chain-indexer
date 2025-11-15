#!/usr/bin/env python3
import json
import time
from pathlib import Path

import requests
from web3 import Web3

# Config
ANVIL_RPC = "http://localhost:8545"
API_BASE = "http://localhost:8080"
TARGET_EVENTS = 1000
TARGET_THROUGHPUT = 10  # events/sec SLO


def load_abi() -> list:
    """Load ABI from TinyToken.abi file."""
    try:
        abi_path = Path("TinyToken.abi")
        with open(abi_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError("TinyToken.abi not found in current directory")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in TinyToken.abi: {e}")


def load_bytecode() -> str:
    """Load bytecode from TinyToken.bin file."""
    try:
        bin_path = Path("TinyToken.bin")
        with open(bin_path, "r") as f:
            bytecode = f.read().strip()
            # Add 0x prefix if not present
            if not bytecode.startswith("0x"):
                bytecode = "0x" + bytecode
            return bytecode
    except FileNotFoundError:
        raise FileNotFoundError("TinyToken.bin not found in current directory")


def deploy_token(w3: Web3, initial_supply: int = 1000000) -> str:
    """Deploy test ERC20 token."""
    abi = load_abi()
    bytecode = load_bytecode()
    account = w3.eth.accounts[0]

    ERC20 = w3.eth.contract(abi=abi, bytecode=bytecode)

    tx_hash = ERC20.constructor(initial_supply).transact({"from": account})
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash)

    return receipt.contractAddress


def generate_transfers(w3: Web3, token_address: str, count: int) -> int:
    """Generate transfer events."""
    abi = load_abi()
    account = w3.eth.accounts[0]
    recipient = w3.eth.accounts[1]

    token = w3.eth.contract(address=token_address, abi=abi)

    start_block = w3.eth.block_number

    for i in range(count):
        tx_hash = token.functions.transfer(recipient, 1).transact({"from": account})
        w3.eth.wait_for_transaction_receipt(tx_hash)

        if (i + 1) % 100 == 0:
            print(f"Generated {i + 1}/{count} transfers")

    end_block = w3.eth.block_number
    print(f"Generated {count} transfers in blocks {start_block}-{end_block}")

    return start_block


def register_contract(token_address: str, start_block: int) -> int:
    """Register contract with deft-indexer."""
    abi = load_abi()
    abi_json = json.dumps(abi)

    resp = requests.post(
        f"{API_BASE}/watch",
        json={
            "address": token_address,
            "network": "anvil",
            "start_block": start_block,
            "abi_json": abi_json,
        },
    )
    resp.raise_for_status()
    return resp.json()["id"]


def wait_for_indexing(
    contract_id: int, target_count: int, timeout: int = 120
) -> tuple[int, float]:
    """Wait for events to be indexed."""
    start = time.time()
    last_count = 0

    while time.time() - start < timeout:
        resp = requests.get(f"{API_BASE}/contracts/{contract_id}/events")
        resp.raise_for_status()
        events = resp.json()

        if len(events) >= target_count:
            duration = time.time() - start
            print(f"Indexed {len(events)} events in {duration:.2f}s")
            return len(events), duration

        if len(events) != last_count:
            print(f"Progress: {len(events)}/{target_count} events")
            last_count = len(events)

        time.sleep(1)

    raise TimeoutError(f"Only indexed {last_count}/{target_count} events in {timeout}s")


def main():
    print("=== deft-indexer Throughput Benchmark ===\n")

    # Connect to Anvil
    w3 = Web3(Web3.HTTPProvider(ANVIL_RPC))
    assert w3.is_connected(), "Cannot connect to Anvil"
    print(f"Connected to Anvil at {ANVIL_RPC}")

    print("Gas limit:", w3.eth.get_block("latest").gasLimit)

    # Deploy token
    print(f"\nDeploying test ERC20 token...")
    token_address = deploy_token(w3)
    print(f"Token deployed at {token_address}")

    # Generate events
    print(f"\nGenerating {TARGET_EVENTS} transfer events...")
    start_block = generate_transfers(w3, token_address, TARGET_EVENTS)

    # Register with indexer
    print(f"\nRegistering contract with deft-indexer...")
    contract_id = register_contract(token_address, start_block)
    print(f"Contract registered with ID {contract_id}")

    # Wait for indexing
    print(f"\nWaiting for indexing to complete...")
    event_count, duration = wait_for_indexing(contract_id, TARGET_EVENTS)

    # Calculate metrics
    throughput = event_count / duration

    print("\n=== Results ===")
    print(f"Events indexed: {event_count}")
    print(f"Duration: {duration:.2f}s")
    print(f"Throughput: {throughput:.2f} events/sec")
    print(f"SLO target: {TARGET_THROUGHPUT} events/sec")

    if throughput >= TARGET_THROUGHPUT:
        print("✅ SLO MET")
        return 0
    else:
        print("❌ SLO MISSED")
        return 1


if __name__ == "__main__":
    exit(main())
