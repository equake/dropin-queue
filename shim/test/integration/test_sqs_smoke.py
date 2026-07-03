"""
test_sqs_smoke.py — smoke test do shim.

Valida o caminho completo:
    boto3 → HTTP POST / → SQS Query protocol → shimd → NATS JetStream

Critério de aceite da Fase 1 / Semana 1:
    - boto3.create_queue() funciona contra o shim
    - URL retornada segue formato AWS (http(s)://endpoint/account/name)
    - Idempotência: criar 2x com mesmo nome devolve mesma URL
"""

import boto3
import pytest
import requests

# Constantes de configuração (mirror do conftest.py).
import os
SHIM_ENDPOINT = os.environ.get("SHIM_ENDPOINT", "http://localhost:4566")
SHIM_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
DEV_ACCESS_KEY = "AKIADEV0000000000000"
DEV_SECRET_KEY = "dev-secret-key-not-validated"


def test_healthz(shim_healthy):
    """Smoke: shim responde /healthz."""
    r = requests.get(f"{SHIM_ENDPOINT}/healthz", timeout=2)
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_readyz(shim_ready):
    """Smoke: shim está conectado ao broker."""
    r = requests.get(f"{SHIM_ENDPOINT}/readyz", timeout=2)
    assert r.status_code == 200
    assert r.json()["status"] == "ready"


def test_metrics_endpoint(shim_ready):
    """Smoke: /metrics responde em formato Prometheus."""
    r = requests.get(f"{SHIM_ENDPOINT}/metrics", timeout=2)
    assert r.status_code == 200
    assert "# HELP" in r.text or "shim_" in r.text


def test_create_queue_returns_aws_format_url(sqs_client, unique_queue_name):
    """Smoke principal: criar fila via boto3 devolve URL formato AWS."""
    resp = sqs_client.create_queue(QueueName=unique_queue_name)
    url = resp["QueueUrl"]

    # URL deve apontar para o shim, conter account ID e nome da fila.
    assert url.startswith(SHIM_ENDPOINT + "/"), f"URL deve começar com {SHIM_ENDPOINT}/, got: {url}"
    assert url.endswith(unique_queue_name), f"URL deve terminar com nome da fila, got: {url}"
    # Deve conter o account ID no meio
    parts = url.split("/")
    assert len(parts) >= 4, f"URL deve ter 4+ partes, got: {url}"
    account = parts[3]
    assert len(account) == 12 and account.isdigit(), f"account ID deve ser 12 dígitos, got: {account}"


def test_create_queue_idempotent(sqs_client, unique_queue_name):
    """Criar fila 2x com mesmo nome devolve mesma URL (sem erro)."""
    r1 = sqs_client.create_queue(QueueName=unique_queue_name)
    r2 = sqs_client.create_queue(QueueName=unique_queue_name)
    assert r1["QueueUrl"] == r2["QueueUrl"]


def test_create_queue_with_attributes(sqs_client, unique_queue_name):
    """Criar fila com atributos customizados deve aceitar e responder."""
    resp = sqs_client.create_queue(
        QueueName=unique_queue_name,
        Attributes={
            "VisibilityTimeout": "60",
            "MessageRetentionPeriod": "86400",  # 1 dia
            "MaximumMessageSize": "65536",       # 64 KB
            "DelaySeconds": "5",
            "ReceiveMessageWaitTimeSeconds": "10",
        },
    )
    assert "QueueUrl" in resp
    # Atributos inválidos devem falhar com InvalidParameterValue
    with pytest.raises(Exception) as exc_info:
        sqs_client.create_queue(
            QueueName=unique_queue_name + "-invalid",
            Attributes={"VisibilityTimeout": "999999"},  # > 43200
        )
    err = exc_info.value
    assert "InvalidParameterValue" in str(err), f"esperava InvalidParameterValue, got: {err}"


def test_create_queue_invalid_name(sqs_client):
    """Nome inválido deve falhar com InvalidParameterValue."""
    with pytest.raises(Exception) as exc_info:
        sqs_client.create_queue(QueueName="queue with spaces and !@# chars")
    err = exc_info.value
    assert "InvalidParameterValue" in str(err), f"esperava InvalidParameterValue, got: {err}"


def test_get_queue_url(sqs_client, unique_queue_name):
    """GetQueueUrl deve devolver a URL da fila criada."""
    created = sqs_client.create_queue(QueueName=unique_queue_name)
    expected_url = created["QueueUrl"]

    resp = sqs_client.get_queue_url(QueueName=unique_queue_name)
    assert resp["QueueUrl"] == expected_url


def test_get_queue_attributes(sqs_client, unique_queue_name):
    """GetQueueAttributes deve devolver atributos da fila."""
    sqs_client.create_queue(
        QueueName=unique_queue_name,
        Attributes={"VisibilityTimeout": "45"},
    )

    resp = sqs_client.get_queue_attributes(
        QueueUrl=f"{SHIM_ENDPOINT}/000000000000/{unique_queue_name}",
        AttributeNames=["All"],
    )

    attrs = resp["Attributes"]
    assert "VisibilityTimeout" in attrs, f"esperava VisibilityTimeout em: {list(attrs.keys())}"
    assert attrs["VisibilityTimeout"] == "45"


def test_create_queue_appears_in_list(sqs_client, unique_queue_name):
    """Fila criada deve aparecer em ListQueues."""
    sqs_client.create_queue(QueueName=unique_queue_name)

    resp = sqs_client.list_queues()
    urls = resp.get("QueueUrls", [])
    assert any(unique_queue_name in u for u in urls), f"fila {unique_queue_name} deve estar em {urls}"


def test_delete_queue(sqs_client, unique_queue_name):
    """DeleteQueue deve remover fila."""
    sqs_client.create_queue(QueueName=unique_queue_name)

    url = f"{SHIM_ENDPOINT}/000000000000/{unique_queue_name}"
    sqs_client.delete_queue(QueueUrl=url)

    # GetQueueUrl deve falhar
    with pytest.raises(Exception) as exc_info:
        sqs_client.get_queue_url(QueueName=unique_queue_name)
    err = str(exc_info.value)
    assert "QueueDoesNotExist" in err or "NonExistentQueue" in err, f"esperava QueueDoesNotExist, got: {err}"


def test_aws_cli_format_compatible():
    """Verifica que o shim aceita o formato exato que aws-cli envia."""
    # aws-cli envia:
    #   POST /
    #   Content-Type: application/x-www-form-urlencoded; charset=utf-8
    #   X-Amz-Date: ...
    #   Body: Action=CreateQueue&QueueName=foo&Version=2012-11-05
    import uuid
    name = f"awscli-{uuid.uuid4().hex[:8]}"

    body = f"Action=CreateQueue&QueueName={name}&Version=2012-11-05"
    r = requests.post(
        SHIM_ENDPOINT + "/",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded; charset=utf-8"},
    )
    assert r.status_code == 200, f"aws-cli format: status {r.status_code}, body: {r.text[:200]}"
    assert "<QueueUrl>" in r.text, f"resposta deve conter <QueueUrl>: {r.text[:200]}"
    assert name in r.text, f"resposta deve conter nome: {r.text[:200]}"
