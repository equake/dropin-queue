"""
conftest.py — fixtures compartilhadas pelos testes de integração.

Os testes rodam contra o shim rodando via docker-compose em http://localhost:4566.
Assume AUTH_MODE=off (dev only) — qualquer credencial é aceita.

Para rodar:
    make up
    pytest shim/test/integration/
"""

import os
import time

import boto3
import pytest
import requests


# --- Constantes ---

SHIM_ENDPOINT = os.environ.get("SHIM_ENDPOINT", "http://localhost:4566")
SHIM_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
SHIM_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID", "000000000000")

# Credenciais dummy aceitas em modo dev (AUTH_MODE=off).
# Em produção, essas credenciais seriam verificadas contra IAM store.
DEV_ACCESS_KEY = "AKIADEV0000000000000"
DEV_SECRET_KEY = "dev-secret-key-not-validated"


# --- Fixtures ---

@pytest.fixture(scope="session")
def shim_endpoint() -> str:
    """URL base do shim."""
    return SHIM_ENDPOINT


@pytest.fixture(scope="session")
def shim_healthy() -> bool:
    """Espera o shim ficar saudável antes de rodar testes."""
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            r = requests.get(f"{SHIM_ENDPOINT}/healthz", timeout=2)
            if r.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(0.5)
    pytest.fail(f"shim não ficou saudável em 30s: {SHIM_ENDPOINT}/healthz")


@pytest.fixture(scope="session")
def shim_ready() -> bool:
    """Espera o shim estar ready (broker conectado)."""
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            r = requests.get(f"{SHIM_ENDPOINT}/readyz", timeout=2)
            if r.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(0.5)
    pytest.fail(f"shim não ficou ready em 30s: {SHIM_ENDPOINT}/readyz")


@pytest.fixture
def sqs_client(shim_ready):
    """Cliente boto3 SQS apontando para o shim."""
    return boto3.client(
        "sqs",
        endpoint_url=SHIM_ENDPOINT,
        region_name=SHIM_REGION,
        aws_access_key_id=DEV_ACCESS_KEY,
        aws_secret_access_key=DEV_SECRET_KEY,
    )


@pytest.fixture
def unique_queue_name():
    """Gera nome único por teste para evitar colisões."""
    import uuid
    return f"test-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def sns_client(shim_ready):
    """Cliente boto3 SNS apontando para o shim."""
    return boto3.client(
        "sns",
        endpoint_url=SHIM_ENDPOINT,
        region_name=SHIM_REGION,
        aws_access_key_id=DEV_ACCESS_KEY,
        aws_secret_access_key=DEV_SECRET_KEY,
    )
