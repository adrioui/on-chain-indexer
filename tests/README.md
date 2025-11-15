# deft-indexer Test Suite

## Overview

This directory contains the automated test suite for deft-indexer, covering API endpoints, Celery tasks, database models, and the ABI resolution service.

## Test Modules

- **`test_api_endpoints.py`** - Happy-path tests for all 6 FastAPI endpoints
- **`test_api_errors.py`** - Error scenario tests (400, 404, 409, 422 responses)
- **`test_abi_resolution.py`** - ABI resolution service unit tests
- **`test_tasks.py`** - Celery task logic tests with mocked Web3
- **`test_models.py`** - Database model constraint and relationship tests
- **`test_api.py`** - Legacy test module (kept for backward compatibility)

## Running Tests

```bash
# Run all tests
pytest

# Run specific module
pytest tests/test_api_endpoints.py

# Run with verbose output
pytest -v

# Run with coverage report
pytest --cov=src/deft_indexer --cov-report=term-missing

# Run specific test
pytest tests/test_api_endpoints.py::test_health_endpoint
```

## Test Patterns

### Database Reset
All test modules use the `reset_database` fixture (autouse=True) to drop and recreate the SQLite schema between tests, ensuring isolation.

### Mocked Dependencies
- **Web3**: `FakeWeb3` class with stubbed `is_address()` and `eth.get_code()`
- **ABI Resolution**: `fake_resolve()` async function returning empty ABI array
- **Celery Tasks**: `fake_delay()` function recording task arguments without execution
- **Kafka Producer**: Context manager mocked to prevent network calls

### Fixture Usage
- `client` - Provides `TestClient` instance with all mocks applied
- `sample_contract` - Creates a pre-registered contract for tests needing existing data
- `mock_web3` - Returns MagicMock configured for task tests
- `reset_database` - Automatically resets database between tests

## Coverage Goals

- API endpoints (`api/main.py`): >85%
- Tasks (`tasks.py`): >75%
- ABI service (`services/abi.py`): >95%
- Models (`models.py`): >90%

## Debugging Tests

```bash
# Run with print statements visible
pytest -s

# Stop on first failure
pytest -x

# Run specific test with detailed output
pytest tests/test_api_errors.py::test_register_contract_duplicate -vv

# Drop into debugger on failure
pytest --pdb
```

## Test Organization

Tests are organized by component to make it easy to locate and maintain them:

1. **API Layer** (`test_api_endpoints.py`, `test_api_errors.py`)
   - Endpoint functionality and error handling
   - Request/response validation
   - HTTP status codes

2. **Service Layer** (`test_abi_resolution.py`)
   - ABI resolution logic
   - External API integration (mocked)
   - Error handling

3. **Task Layer** (`test_tasks.py`)
   - Celery task execution
   - Checkpoint management
   - Event processing

4. **Data Layer** (`test_models.py`)
   - Database constraints
   - Cascade deletes
   - Relationships

## Adding New Tests

When adding new tests, follow these guidelines:

1. Place tests in the appropriate module based on what's being tested
2. Use existing fixtures for database and mocked dependencies
3. Write descriptive test names that explain what's being validated
4. Add docstrings to tests explaining the scenario
5. Ensure tests are isolated and don't depend on execution order
6. Mock external dependencies (Web3, Kafka, etc.)

Example:
```python
def test_my_new_feature(client):
    """Test that my new feature correctly handles X scenario."""
    test_client, _ = client
    
    # Test implementation
    response = test_client.get("/my-endpoint")
    assert response.status_code == 200
```

## CI/CD Integration

These tests are designed to run in CI/CD pipelines:
- No external service dependencies (uses SQLite, mocks for Web3/Kafka)
- Fast execution (<5 seconds for full suite)
- Clear failure messages
- Exit code 0 on success, non-zero on failure
